package main

import (
	"context"
	"fmt"

	"consul-server-discovery/discovery"

	"github.com/hashicorp/go-hclog"
)

func main() {
	w := &discovery.Watcher{
		Log: hclog.New(&hclog.LoggerOptions{
			Name:  "server-discovery",
			Level: hclog.Debug,
		}),
		Config: discovery.Config{
			Addresses: "exec=./get-docker-addrs.sh",
			GRPCPort:  8502,
			TLSConfig: discovery.TLSConfig{
				CACertPath: "../learn-consul-docker/datacenter-deploy-secure/certs/consul-agent-ca.pem",
				ServerName: "127.0.0.1",
			},
		},
	}

	// Important: subscribe prior to Run to ensure the channel receives the first update.
	ch := w.Chan()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	for {
		ips, ok := <-ch
		if !ok {
			// channel was closed.
			return
		}
		fmt.Printf("server ips from chan: %s\n", ips)
	}
}
