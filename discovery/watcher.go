package discovery

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"consul-server-discovery/proto/pbdataplane"
	"consul-server-discovery/proto/pbserverdiscovery"

	"github.com/hashicorp/go-hclog"
	netaddrs "github.com/hashicorp/go-netaddrs"
)

type Config struct {
	Addresses string
	GRPCPort  int

	RetryInterval time.Duration
	RetryTimeout  time.Duration

	TLSConfig
	Credentials
}

type TLSConfig struct {
	CACertPath string
	ServerName string
	// TODO: add other fields I didn't need yet
}

type Credentials struct {
	Token string
}

type Watcher struct {
	config Config
	log    hclog.Logger

	// addrs is the set of server ips. this tracks whether a server is healthy or not.
	// this is a custom type that is safe for concurrent use, though that may not be strictly required.
	addrs *ServerSet

	// chans is a list of subscribers. each time the server set changes, we send the current server set to every channel.
	// subscribers must consume from these channels or our sends will be dropped and they will miss updates.
	chans []chan ServerInfo

	// tls is the tls config for gRPC connections
	tls credentials.TransportCredentials
	// the current grpc connection
	conn          *grpc.ClientConn
	currentServer ServerState
	token         string
}

type ServerInfo struct {
	// CurrentServer is the server the library is currently connected to.
	// (or at least, it was at the time this struct was sent on the channel)
	CurrentServer ServerState

	// AllServers is the list of all server addresses.
	AllServers []ServerState

	GRPCConn grpc.ClientConnInterface

	// The Consul ACL token.
	Token string
}

type ServerAddr struct {
	IP   string
	Port int
}

func (s ServerAddr) String() string {
	return fmt.Sprintf("%s:%d", s.IP, s.Port)
}

func NewWatcher(config Config, log hclog.Logger) (*Watcher, error) {
	var tls credentials.TransportCredentials
	if config.TLSConfig.CACertPath != "" {
		cred, err := credentials.NewClientTLSFromFile(
			config.TLSConfig.CACertPath,
			config.TLSConfig.ServerName,
		)
		if err != nil {
			return nil, fmt.Errorf("invalid cert config: %w", err)
		}
		tls = cred
	}

	return &Watcher{
		log:    log,
		config: config,
		addrs:  NewServerSet(),
		tls:    tls,
		token:  config.Credentials.Token,
	}, nil
}

func (w *Watcher) Chan() chan ServerInfo {
	// Add a channel. Every time there's a server set change, we sent current server ips to the channel.
	// Callers must continually check the channel to prevent it from filling up.
	//
	// TODO: How would a caller unsubscribe? This probably isn't a practical concern for us.
	ch := make(chan ServerInfo, 1)
	w.chans = append(w.chans, ch)
	return ch
}

func (w *Watcher) refreshAddrs(ctx context.Context) error {
	addresses, err := netaddrs.IPAddrs(context.Background(), w.config.Addresses, w.log)
	if err != nil {
		w.log.Error("retrieving server addresses", "err", err)
		return err
	}

	// TODO: The first time we call this, we don't really know which servers are "good"
	// or not. But we mark them healthy in order to consider them.
	addrs := []ServerAddr{}
	for _, addr := range addresses {
		addrs = append(addrs, ServerAddr{
			IP:   addr.String(),
			Port: w.config.GRPCPort,
		})
	}
	w.addrs.SetKnownHealthy(addrs...)
	return nil
}

func (w *Watcher) getReadyAddr(ctx context.Context) (ServerAddr, error) {
	find := func() ServerAddr {
		addrs := w.addrs.Get(Healthy)
		w.log.Debug("find", "addrs", addrs)
		if len(addrs) > 0 {
			return addrs[0].Addr
		}
		return ServerAddr{}
	}

	addr := find()
	if addr.IP != "" {
		return addr, nil
	}

	err := retryForever(ctx, 1*time.Second, func() error {
		w.log.Debug("retry refresh addrs")
		err := w.refreshAddrs(ctx)
		if err != nil {
			return err
		}
		addr = find()
		if addr.IP != "" {
			return nil
		}
		return fmt.Errorf("no ready servers")
	})
	if err != nil {
		return ServerAddr{}, err
	}
	return addr, nil
}

// connect sets w.currentServer and w.conn on success
func (w *Watcher) connect(ctx context.Context, addr ServerAddr) error {
	var dialOpts []grpc.DialOption
	if w.tls != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(w.tls))
	}
	//dialOpts = append(dialOpts, grpc.WithTimeout(1*time.Second))

	w.currentServer = ServerState{
		Status:   Healthy,
		Features: map[string]bool{},
		Addr:     addr,
	}
	defer func() {
		if w.currentServer.Status == Unhealthy {
			w.closeConn()
		}
		w.addrs.Update(w.currentServer)
	}()

	err := retryTimeout(ctx, 500*time.Millisecond, 5*time.Second, func() error {
		w.log.Debug("retry dial", "addr", addr)
		conn, err := grpc.DialContext(ctx, addr.String(), dialOpts...)
		if err != nil {
			return err
		}
		w.conn = conn
		return nil
	})
	if err != nil {
		w.currentServer.Status = Unhealthy
		return err
	}

	err = retryTimeout(ctx, 2*time.Second, 10*time.Second, func() error {
		w.log.Debug("retry server features")
		// When we reconnect, recheck server features in case the server was reconfigured?
		// (Since we store server features, we could potentially skip this.)
		client := pbdataplane.NewDataplaneServiceClient(w.conn)
		features, err := client.GetSupportedDataplaneFeatures(ctx, &pbdataplane.GetSupportedDataplaneFeaturesRequest{})
		if err != nil {
			return fmt.Errorf("checking supported features: %w", err)
		}

		// Translate features to a map. Mostly so that we don't have to pass these gRPC
		// types back to users.
		for _, feat := range features.SupportedDataplaneFeatures {
			nameStr := pbdataplane.DataplaneFeatures_name[int32(feat.FeatureName)]
			supported := feat.GetSupported()
			w.log.Debug("feature", "supported", supported, "name", nameStr)
			w.currentServer.Features[nameStr] = supported
		}
		return nil
	})
	if err != nil {
		w.currentServer.Status = Unhealthy
		return err
	}
	return nil
}

func (w *Watcher) Run(ctx context.Context) {
	// TODO: Really, you should not call Run() twice.
	defer w.cleanup()

	// TODO: Also support ACL token login through the gRPC endpoint.
	// For now, using a statically configured ACL token.
	// I think since we use DialContext, the token will also be included
	// in the conn returned on the channel to subscribers.
	if w.token != "" {
		w.log.Debug("using token")
		ctx = metadata.AppendToOutgoingContext(ctx, "x-consul-token", w.token)
	}

	for {
		select {
		case <-ctx.Done():
			w.log.Info("done. exiting")
			return
		default:
			// getReadyAddr will retry forever.
			// It uses a known "healthy" server from w.addrs, if one exists.
			// Otherwise, it runs the netaddrs discovery and, on success, marks those servers as healthy.
			// We'll attempt to connect to each "healthy" server at least once. When connecting fails,
			// we mark the server "unhealthy" in w.addrs. If all servers are marked "unhealthy",
			// then we'll arrive back to getReadyAddr, which will start from the beginning.
			//
			// TODO: this is confusing logic to follow. make this better.
			addr, err := w.getReadyAddr(ctx)
			if err != nil {
				w.log.Error("no ready addresses", "err", err)
				break // break select
			}

			// Always try to connect and check for server watch support.
			// This retries for a short period, to account for transient issues.
			// This sets w.conn and w.currentServer on success.
			// On failure, it marks the server addr "unhealthy". The loop returns to getReadyAddr
			// which selects a new "healthy" server to try.
			err = w.connect(ctx, addr)
			if err != nil {
				w.log.Error("connecting", "addr", addr, "err", err)
				break // break select
			}

			if w.currentServer.Features["DATAPLANE_FEATURES_WATCH_SERVERS"] {
				// watchServers opens a gRPC stream to receive server set changes.
				// watchServers marks the servers returned from this stream "healthy",
				// and marks all others "unhealthy".
				//
				// On an error that aborts the gRPC stream, it closes the connection and
				// marks that server "unhealthy".
				w.watchServers(ctx)
			} else {
				// If the server watch is not supported, then we want to periodically re-run the
				// netaddrs server discovery in order to try to keep a fresh list of servers.
				w.log.Error("watch servers not supported", addr, w.currentServer.Addr.String())
				if addr.IP != "" {
					// TODO: only call this if there is actually a change.
					w.onServerSetChanged()
				}
				<-time.After(5 * time.Second)
			}
		}
	}
}

func (w *Watcher) cleanup() {
	for _, ch := range w.chans {
		close(ch)
	}
	w.chans = nil
	w.closeConn()
}

func (w *Watcher) closeConn() {
	if w.conn != nil {
		err := w.conn.Close()
		if err != nil {
			w.log.Warn("closing connection", "err", err)
		}
		w.conn = nil
	}
}

func (w *Watcher) watchServers(ctx context.Context) {
	// Whenever we return, mark the server unhealthy. The only case we don't
	// want to do this when the process is exiting, but that's not a concern
	// since we track servers in memory except that it will log the error.
	defer func() {
		w.log.Error("marking server unhealthy", "addr", w.currentServer.Addr)
		// We don't notify subscribers of the change here. Instead, we will notify
		// them after connecting to a "good" server.
		w.closeConn()

		w.currentServer.Status = Unhealthy
		w.addrs.Update(w.currentServer)
	}()

	client := pbserverdiscovery.NewServerDiscoveryServiceClient(w.conn)
	serverStream, err := client.WatchServers(ctx, &pbserverdiscovery.WatchServersRequest{Wan: false})
	if err != nil {
		w.log.Error("failed to watch server", "err", err)
		return
	}

	for {
		// This blocks until there is some change. Does this ever time out? Seems like no.
		// The docstring of the underlying RecvMsg said it can return io.EOF on success.
		// (I haven't seen this happen so far, so I wonder if it doesn't apply to a stream).
		//
		// The underlying connection is created with DialContext to support canceling
		// this using the context. The context includes the ACL token as well.
		resp, err := serverStream.Recv()
		if err != nil {
			w.log.Error("watching servers", "err", err)
			return
		}

		// This marks the servers we've found healthy. And marks any others unhealthy.
		addrs := []ServerAddr{}
		for _, srv := range resp.Servers {
			addrs = append(addrs, ServerAddr{
				IP:   srv.Address,
				Port: w.config.GRPCPort,
			})
		}
		w.addrs.SetKnownHealthy(addrs...)
		w.onServerSetChanged()
	}
}

func (w *Watcher) onServerSetChanged() {
	// Technically, we might call this even if the servers haven't changed.
	addrs := w.addrs.Get(Healthy)

	w.log.Debug("onServerSetChanged", "good-addrs", addrs)

	info := ServerInfo{
		CurrentServer: w.currentServer,
		AllServers:    addrs,
		GRPCConn:      w.conn,
		Token:         w.token,
	}

	// Notify subscribers of server ips.
	for _, ch := range w.chans {
		select {
		case ch <- info:
			// sent!
		default:
			// failed to send.
			// if the user has not consumed from the channel, the buffer may be full and this update is lost.
			// One option is to resend server state to blocked channels periodically.
			// An alternative is to require clients to constantly check the channel, and use a larger channel (size = 10).
			// Then they _should_ typically have an empty channel.
		}
	}
}
