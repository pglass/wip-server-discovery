package discovery

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"

	"consul-server-discovery/proto/pbdataplane"
	"consul-server-discovery/proto/pbserverdiscovery"

	"github.com/hashicorp/go-hclog"
	netaddrs "github.com/hashicorp/go-netaddrs"
)

type Watcher struct {
	config Config
	log    hclog.Logger

	// addrs is the set of server ips. this tracks whether a server is healthy or not.
	// this is a custom type that is safe for concurrent use.
	addrs *ServerSet

	// chans is a list of subscribers. each time the current server changes, we
	// an update to each channel. A subscriber must consume from its channel or else
	// sends will be dropped and they will miss updates.
	chans []chan SubscribeStuff

	// dialOpts are options for grpc.Dial. This is set once in the constructor
	// and is read-only after that point.
	dialOpts []grpc.DialOption

	// The grpc connection. By using a custom resolver, we use the same connection
	// object forever.
	conn *grpc.ClientConn

	// Set to true once init() has finished successfully.
	initComplete atomic.Value

	// Other stuff that callers need access to. These are set once after init()
	// and are read-only after that point.
	dataplaneFeatures map[string]bool
	token             string

	// The current server that we are connected to.
	// This changes over time as we encounter connection errors or are told to switch servers.
	currentServer ServerState

	// State for the grpc resolver that this type implements.
	clientConn resolver.ClientConn

	// State for the custom Balancer
	balancer *balancerWrapper
}

// Watcher implements these so we can use it as a grpc resolver.
var _ resolver.Builder = (*Watcher)(nil)
var _ resolver.Resolver = (*Watcher)(nil)

func NewWatcher(config Config, log hclog.Logger) (*Watcher, error) {
	var dialOpts []grpc.DialOption
	if config.TLS.CACertsPath != "" {
		cred, err := credentials.NewClientTLSFromFile(
			config.TLS.CACertsPath,
			config.TLS.ServerName,
		)
		if err != nil {
			return nil, fmt.Errorf("invalid cert config: %w", err)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(cred))
	}

	w := &Watcher{
		log:    log,
		config: config,
		addrs:  NewServerSet(),
		token:  config.Credentials.Static.Token,
	}

	// Configure our custom gRPC resolver and load balancer.
	// These enable us to switch out the address on the connection.
	dialOpts = append(dialOpts,
		grpc.WithResolvers(w),
		grpc.WithDefaultServiceConfig(
			fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, balancerPolicyName),
		),
	)
	w.dialOpts = dialOpts

	w.initComplete.Store(false)
	return w, nil
}

// InitStuff contains the info a caller may need to know
// after initialization is completed.
type InitStuff struct {
	GRPConn           grpc.ClientConnInterface
	Token             string
	Address           ServerAddr
	DataplaneFeatures map[string]bool
}

// waitForInit waits for initialization to complete and returns stuff
// the caller may need: ACL token, gRPC connection
//
// The connection is good to use "forever" (as long as this Watcher is valid).
// It is configured with a resolver that automatically updates the server address
// the connection is using. The returned connection is shared with this Watcher.
func (w *Watcher) waitForInit(ctx context.Context) (*InitStuff, error) {
	w.log.Debug("Watcher.waitForInit")
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			// Wait for initialization to complete.
			time.Sleep(250 * time.Millisecond)
			if w.initComplete.Load().(bool) {
				return &InitStuff{
					GRPConn:           w.conn,
					Token:             w.token,
					DataplaneFeatures: w.dataplaneFeatures,
					Address:           w.currentServer.Addr,
				}, nil

			}
		}
	}
}

// SubscribeStuff is the info returned to a subscriber whenever there is a
// change they sholuld know about.
type SubscribeStuff struct {
	// Address is the current server address the Watcher is using.
	Address ServerAddr
}

// Subscribe stores returns channel. This Watcher will return the current
// server address on the channel whenever the address changes.
func (w *Watcher) Subscribe() chan SubscribeStuff {
	// Add a channel. Every time there's a server set change, we sent current server ips to the channel.
	// Callers must continually check the channel to prevent it from filling up.
	//
	// TODO: How would a caller unsubscribe? This probably isn't a practical concern for us.
	ch := make(chan SubscribeStuff, 1)
	w.chans = append(w.chans, ch)
	if w.currentServer.Addr.IsValid() {
		// always populate with the current server initially.
		ch <- SubscribeStuff{Address: w.currentServer.Addr}
	}
	return ch
}

// discoverAddrs discovers addresses using go-netaddrs (DNS or exec command)
// and marks exactly those addresses "healthy". This will retry forever until
// success or until the context is cancelled.
func (w *Watcher) discoverAddrs(ctx context.Context) error {
	w.log.Debug("Watcher.discoverAddrs")
	var addresses []net.IPAddr
	// TODO: exponential backoff.
	err := retryForever(ctx, 1*time.Second, func() error {
		addrs, err := netaddrs.IPAddrs(ctx, w.config.Addresses, w.log)
		if err != nil {
			w.log.Error("retrieving server addresses", "err", err)
			return err
		}
		addresses = addrs
		return nil
	})
	if err != nil {
		return err
	}

	// TODO: The first time we call this, we don't really know which servers are "good"
	// or not. But we mark them healthy in order to consider them.
	addrs := []ServerAddr{}
	for _, addr := range addresses {
		addrs = append(addrs, MakeServerAddr(addr, w.config.GRPCPort))
	}
	w.addrs.SetKnownHealthy(addrs...)
	return nil
}

// selectAddr chooses a healthy server to connect to.
//
// It will reuse the current server (w.currentServer) if it is still healthy.
// If there are any known healthy servers, it will choose one of those.
// If there are no healthy servers, it reruns discoverAddrs until success.
func (w *Watcher) selectAddr(ctx context.Context) (ServerAddr, error) {
	w.log.Debug("Watcher.selectAddr")
	find := func() ServerAddr {
		addrs := w.addrs.Get(Healthy)
		w.log.Debug("known healthy servers", "addrs", addrs)
		// Prefer reusing the current server if it's healthy.
		if w.currentServer.Addr.IsValid() {
			for _, a := range addrs {
				if a.Addr == w.currentServer.Addr {
					return a.Addr
				}
			}
		}
		// Otherwise, pick any healthy server.
		if len(addrs) > 0 {
			return addrs[0].Addr
		}
		return ServerAddr{}
	}

	// Find a server from memory.
	addr := find()
	if addr.IsValid() {
		return addr, nil
	}

	// Refresh list of servers.
	err := w.discoverAddrs(ctx)
	if err != nil {
		return ServerAddr{}, err
	}

	// Now find one.
	addr = find()
	if addr.IsValid() {
		return addr, nil
	}
	return ServerAddr{}, fmt.Errorf("no known healthy servers")
}

func (w *Watcher) init(ctx context.Context) error {
	w.log.Debug("Watcher.init")
	defer func() {
		w.addrs.Update(w.currentServer)
	}()

	if !w.initComplete.Load().(bool) {
		// Create the grpc connection.
		if w.conn == nil {
			err := retryTimeout(ctx, 500*time.Millisecond, 5*time.Second, func() error {
				w.log.Debug("retry: grpc.Dial")

				// We pass an address string that only contains the "consul" scheme in order to
				// trigger our custom resolver. We don't pass an actual address (ip/port) since
				// the resolver will set that.
				conn, err := grpc.DialContext(ctx, "consul://", w.dialOpts...)
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
		}

		// Grab server features.
		if w.dataplaneFeatures == nil {
			err := retryTimeout(ctx, 2*time.Second, 10*time.Second, func() error {
				w.log.Debug("retry: get supported dataplane features")
				// When we reconnect, recheck server features in case the server was reconfigured?
				// (Since we store server features, we could potentially skip this.)

				client := pbdataplane.NewDataplaneServiceClient(w.conn)
				features, err := client.GetSupportedDataplaneFeatures(ctx, &pbdataplane.GetSupportedDataplaneFeaturesRequest{})
				if err != nil {
					return fmt.Errorf("checking supported features: %w", err)
				}

				// Translate features to a map, so that we don't have to pass gRPC
				// types back to users?
				w.dataplaneFeatures = map[string]bool{}
				for _, feat := range features.SupportedDataplaneFeatures {
					nameStr := pbdataplane.DataplaneFeatures_name[int32(feat.FeatureName)]
					supported := feat.GetSupported()
					w.log.Debug("feature", "supported", supported, "name", nameStr)
					w.dataplaneFeatures[nameStr] = supported
				}
				return nil
			})
			if err != nil {
				w.currentServer.Status = Unhealthy
				return err
			}
		}

		// init complete!
		w.initComplete.Store(true)
		w.log.Debug("Watcher.init complete")
	}

	return nil
}

func (w *Watcher) Run(ctx context.Context) (*InitStuff, error) {
	// TODO: remove this. just a debug hack to see connection state changes.
	go func() {
		var state connectivity.State
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Millisecond):
				if w.conn != nil {
					current := w.conn.GetState()
					if state != current {
						w.log.Debug("state change", "from", state, "to", current)
						state = current
					}

				}
			}
		}
	}()

	// initialize custom balancer
	// TODO: this is a global registration
	balancer.Register(newBalancerBuilder(w))

	// TODO: Really, you should not call Run() twice.
	go w.run(ctx)
	return w.waitForInit(ctx)
}

func (w *Watcher) run(ctx context.Context) {
	defer w.cleanup()

	// TODO: Also support ACL token login through the gRPC endpoint.
	// For now, using a statically configured ACL token.
	//
	// This works because we pass this context into each request.
	// The "x-consul-token" metadata is not stored on the connection obj.
	if w.token != "" {
		w.log.Debug("using token")
		ctx = metadata.AppendToOutgoingContext(ctx, "x-consul-token", w.token)
	}

	for {
		select {
		case <-ctx.Done():
			w.log.Info("done. exiting")
			if err := ctx.Err(); err != nil {
				w.log.Warn("context error", "err", err)
			}
			return
		default:
			// selectAddr picks a server to use. It picks from memory first, or reruns
			// go-netaddrs server discovery.
			addr, err := w.selectAddr(ctx)
			if err != nil {
				w.log.Error("selecting server address", "err", err)
				break // break select
			}

			err = w.changeServer(addr)
			if err != nil {
				w.log.Error("failed switching to server", "addr", addr.String(), "err", err)
				break // break select
			}

			err = w.init(ctx)
			if err != nil {
				w.log.Error("init", "err", err.Error())
				break // break select
			}

			if w.dataplaneFeatures["DATAPLANE_FEATURES_WATCH_SERVERS"] {
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
				w.log.Error("watch servers not supported")
				<-time.After(5 * time.Second)
			}
		}
	}
}

func (w *Watcher) changeServer(to ServerAddr) error {
	w.log.Debug("Watcher.changeServer", "to", to)
	w.currentServer = ServerState{
		Status: Healthy,
		Addr:   to,
	}
	w.updateClientConnAddr()

	// Tell the connection to switch to the new server.
	// This blocks until the transition is done.

	if w.initComplete.Load().(bool) {
		w.log.Debug("Watcher.changeServer (waiting for transition)")
		err := w.balancer.WaitForTransition(to)
		if err != nil {
			w.currentServer.Status = Unhealthy
			w.addrs.Update(w.currentServer)

			// tell the conn to stop using this address.
			w.currentServer = ServerState{}
			w.updateClientConnAddr()
			return err
		}
	}

	w.notifyCallers()
	return nil
}

func (w *Watcher) cleanup() {
	for _, ch := range w.chans {
		close(ch)
	}
	w.chans = nil

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
		// them after choosing the next "good" server.
		w.currentServer.Status = Unhealthy
		w.addrs.Update(w.currentServer)
	}()

	client := pbserverdiscovery.NewServerDiscoveryServiceClient(w.conn)
	// TODO: wan false/true?
	serverStream, err := client.WatchServers(ctx, &pbserverdiscovery.WatchServersRequest{Wan: false})
	if err != nil {
		w.log.Error("failed to watch server", "err", err)
		return
	}

	for {
		// This blocks until there is some change.
		resp, err := serverStream.Recv()
		if err != nil {
			w.log.Error("unable to receive from server watch stream", "err", err)
			return
		}

		// This marks the servers we've found healthy. And marks any others unhealthy.
		//
		// TODO: Is there a case where the server watch stream does not return the server we
		// are currently connected to? Assuming not for now.
		addrs := []ServerAddr{}
		for _, srv := range resp.Servers {
			w.log.Debug("server", "addr", srv.Address)
			a, err := MakeServerAddrStr(srv.Address, w.config.GRPCPort)
			if err != nil {
				w.log.Error("invalid address", "addr", srv.Address, "err", err)
				continue
			}
			addrs = append(addrs, a)
		}
		w.addrs.SetKnownHealthy(addrs...)
	}
}

func (w *Watcher) notifyCallers() {
	// Notify subscribers of server ips.
	for _, ch := range w.chans {
		select {
		case ch <- SubscribeStuff{Address: w.currentServer.Addr}:
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

// Build implements resolver.Builder
//
// This is called by gRPC synchronously we grpc.Dial.
func (w *Watcher) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	w.log.Debug("Watcher.Build (custom resolver)", "cc", fmt.Sprintf("%T", cc), "target", target.URL.String(), "opts", fmt.Sprintf("%+v", opts))

	if w.clientConn != nil {
		w.log.Warn("Watcher.Build (custom resolver) called more than once")
	}

	// we use this later. The grpc dial calls Build synchronously, so this is safe as long as you don't dial from two goroutines...
	w.clientConn = cc
	w.updateClientConnAddr()
	return w, nil
}

// Scheme implements resolver.Builder
func (w *Watcher) Scheme() string {
	w.log.Debug("Watcher.Scheme (custom resolver)")
	// so that we can do grpc.Dial("consul://")? I guess?
	return "consul"
}

// Close implements resolver.Resolver
func (w *Watcher) Close() {}

// ResolveNow implements resolver.Resolver
//
// "ResolveNow will be called by gRPC to try to resolve the target name
// again. It's just a hint, resolver can ignore this if it's not necessary.
// It could be called multiple times concurrently."
func (w *Watcher) ResolveNow(_ resolver.ResolveNowOptions) {}

func (w *Watcher) updateClientConnAddr() {
	if w.clientConn == nil {
		w.log.Warn("Watcher.updateClientConnAddr has no clientConn (skip)")
		return
	}
	w.log.Debug("Watcher.updateClientConnAddr")

	// note: we need this to work prior to initComplete.
	var addrs []resolver.Address
	if addr := w.currentServer.Addr; addr.IsValid() {
		addrs = append(addrs, resolver.Address{Addr: addr.String()})
	}
	// In case we connect to a server, and then all servers go away,
	// we'll update this to an empty list of addresses.
	err := w.clientConn.UpdateState(resolver.State{Addresses: addrs})
	if err != nil {
		w.log.Error("update client conn state", "err", err)
	}

}
