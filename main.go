package main

import (
	"context"
	"log"
	"time"

	"consul-server-discovery/discovery"
	"consul-server-discovery/proto/pbdataplane"

	"github.com/hashicorp/go-hclog"
	"google.golang.org/grpc/metadata"
)

var testToken = "0f550883-d862-49a8-9603-4ce3c91a10c8"

func main() {
	w, err := discovery.NewWatcher(
		discovery.Config{
			Addresses: "exec=./get-docker-addrs.sh",
			GRPCPort:  8502,
			TLS: discovery.TLSConfig{
				CACertsPath: "../learn-consul-docker/datacenter-deploy-secure/certs/consul-agent-ca.pem",
				ServerName:  "127.0.0.1",
			},
			Credentials: discovery.Credentials{
				Static: discovery.StaticTokenCredential{
					Token: testToken,
				},
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// This channel will receive an update each time the current server changes.
	ch := w.Subscribe()

	// Start the Watcher. This starts a the Watcher's goroutine.
	// It waitsTLSConfig for the server discovery initialization to happen,
	// and returns "init stuff", like gRPC connection object, ACL token,
	//
	// The gRPC connection is configured with a resolver that automatically
	// changes the connection's address when it needs to switch servers.
	// The connection can therefore be used "forever".
	initStuff, err := w.Run(ctx)
	if err != nil {
		log.Fatalf("server discovery: %s", err)
	}
	log.Println("dial success!")

	// TODO: can we avoid passing this for every request?
	reqCtx := metadata.AppendToOutgoingContext(ctx, "x-consul-token", initStuff.Token)
	client := pbdataplane.NewDataplaneServiceClient(initStuff.GRPConn)

	for {
		select {
		case addr, ok := <-ch:
			if !ok {
				// channel was closed.
				return
			}
			log.Printf("current server from chan: %s\n", addr)
		case <-time.After(3 * time.Second):
			// Sample gRPC request (using protos I currently have)
			_, err := client.GetSupportedDataplaneFeatures(reqCtx, &pbdataplane.GetSupportedDataplaneFeaturesRequest{})
			if err != nil {
				log.Printf("grpc request failed: %v", err)
			} else {
				log.Printf("grpc request succeeded")
			}
		}
	}
}
