package discovery

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"consul-server-discovery/proto/pbserverdiscovery"

	"github.com/hashicorp/go-hclog"
	netaddrs "github.com/hashicorp/go-netaddrs"
)

type Config struct {
	Addresses string
	GRPCPort  int
	TLSConfig
}

type TLSConfig struct {
	CACertPath string
	ServerName string
	// TODO: add other fields I didn't need yet
}

type Watcher struct {
	Config
	Log hclog.Logger

	// set of server ips. this tracks whether a server is healthy or not.
	// this is a custom type that is safe for concurrent use, though that may not be strictly required.
	addrs *ServerSet

	chans []chan ServerIPs
}

func (w *Watcher) Chan() chan ServerIPs {
	// Add a channel. Every time there's a server set change, we sent current server ips to the channel.
	// Callers must continually check the channel to prevent it from filling up.
	//
	// TODO: How would a caller unsubscribe? This probably isn't a practical concern for us.
	ch := make(chan ServerIPs, 1)
	w.chans = append(w.chans, ch)
	return ch
}

func (w *Watcher) refreshAddrs(ctx context.Context) error {
	addresses, err := netaddrs.IPAddrs(context.Background(), w.Config.Addresses, w.Log)
	if err != nil {
		w.Log.Error("retrieving server addresses", "err", err)
		return err
	}

	// TODO: The first time we call this, we don't really know which servers are "good"
	// or not. But we mark them healthy in order to consider them.
	addrs := []string{}
	for _, addr := range addresses {
		addrs = append(addrs, addr.String())
	}
	w.addrs.SetKnownHealthy(addrs...)
	return nil
}

func (w *Watcher) getReadyAddr(ctx context.Context) (string, error) {
	find := func() string {
		addrs := w.addrs.Get(Healthy)
		if len(addrs) > 0 {
			return addrs[0]
		}
		return ""
	}

	addr := find()
	if addr != "" {
		return addr, nil
	}
	w.refreshAddrs(ctx)
	addr = find()
	if addr != "" {
		return addr, nil
	}
	// TODO: Probably we want this to retry forever rather than exit.
	return "", fmt.Errorf("no ready servers")
}

func (w *Watcher) Run(ctx context.Context) {
	// TODO: Really, you should not call Run() twice.
	defer w.cleanup()

	if w.addrs == nil {
		w.addrs = NewServerSet()
	}

	for {
		select {
		case <-ctx.Done():
			w.Log.Info("done. exiting")
			return
		default:
			ip, err := w.getReadyAddr(ctx)
			if err != nil {
				w.Log.Error("no ready addresses; aborting")
				return
			}
			w.waitForChanges(ctx, ip)
		}
	}
}

func (w *Watcher) cleanup() {
	for _, ch := range w.chans {
		close(ch)
	}
}

func (w *Watcher) waitForChanges(ctx context.Context, ip string) {
	// Whenever we return, mark the server unhealthy. The only case we don't
	// want to do this when the process is exiting, but that's not a concern
	// since we track servers in memory except that it will log the error.
	defer func() {
		w.Log.Error("marking server unhealthy", "ip", ip)
		// TODO: When we do this, we should also notify channels of the change.
		// Technically, this would happen on next time we wait for changes.
		w.addrs.Set(ip, Unhealthy)
		w.Log.Debug("w.addrs", "val", w.addrs.String())
	}()

	addr := fmt.Sprintf("%s:%d", ip, w.GRPCPort)

	// TODO: don't do this here
	tls, err := credentials.NewClientTLSFromFile(
		w.TLSConfig.CACertPath,
		w.TLSConfig.ServerName,
	)
	if err != nil {
		w.Log.Error("invalid cert config", "err", err)
		return
	}

	w.Log.Info("dialing addr", "addr", addr)
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(tls))
	if err != nil {
		w.Log.Error("failed to dial server", "addr", addr, "err", err)
		return
	}

	client := pbserverdiscovery.NewServerDiscoveryServiceClient(conn)
	serverStream, err := client.WatchServers(ctx, &pbserverdiscovery.WatchServersRequest{Wan: false})
	if err != nil {
		w.Log.Error("failed to watch server", "addr", addr, "err", err)
		return
	}
	defer conn.Close()

	for {
		// This blocks until there is some change. Does this ever time out? Seems like no.
		// The docstring of the underlying RecvMsg said it can return io.EOF on success
		// (I haven't seen this happen so far, so I wonder if it doesn't apply to a stream).
		//
		// The underlying connection is created with DialContext to support canceling
		// this using the context.
		resp, err := serverStream.Recv()
		if err != nil {
			w.Log.Error("failed to recv server addrs", "err", err)
			return
		}
		w.Log.Info("received servers", "servers", resp.Servers)

		// This marks the servers we've found healthy. And marks any others unhealthy.
		addrs := []string{}
		for _, srv := range resp.Servers {
			addrs = append(addrs, srv.Address)
		}
		w.addrs.SetKnownHealthy(addrs...)
		w.Log.Debug("w.addrs", "val", w.addrs)

		w.onServerSetChanged()
	}
}

func (w *Watcher) onServerSetChanged() {
	// Technically, we might call this even if the servers haven't changed.
	ips := w.addrs.Get(Healthy)

	w.Log.Debug("onServerSetChanged", "good-ips", ips)

	// Notify subscribers of server ips.
	for _, ch := range w.chans {
		select {
		case ch <- ips:
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
