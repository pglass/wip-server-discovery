package main

import (
	"context"
	"log"

	"consul-server-discovery/discovery"

	"github.com/hashicorp/go-hclog"
)

var testToken = "0f550883-d862-49a8-9603-4ce3c91a10c8"

func main() {
	w, err := discovery.NewWatcher(
		discovery.Config{
			Addresses: "exec=./get-docker-addrs.sh",
			GRPCPort:  8502,
			TLSConfig: discovery.TLSConfig{
				CACertPath: "../learn-consul-docker/datacenter-deploy-secure/certs/consul-agent-ca.pem",
				ServerName: "127.0.0.1",
			},
			Credentials: discovery.Credentials{
				Token: testToken,
			},
		},
		hclog.New(&hclog.LoggerOptions{
			Name:  "server-discovery",
			Level: hclog.Debug,
		}),
	)

	if err != nil {
		log.Fatal(err)
	}

	// Important: subscribe prior to Run to ensure the channel receives the first update.
	ch := w.Chan()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	for {
		info, ok := <-ch
		if !ok {
			// channel was closed.
			return
		}
		log.Printf("server addrs from chan: %+v\n", info.AllServers)
	}
}
