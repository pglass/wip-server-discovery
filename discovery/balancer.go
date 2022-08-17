package discovery

// This implements a custom gRPC balancer. We don't actually load balancer with it.
// We use the custom balancer has a hook to view sub-conn state and addresses so
// that we can know when a connection is done switching from one server to another
// (since there doesn't seem to give as an easier way to find that, or otherwise
// know which server a response/error came from).
//
// We use this in a specific way with the gRPC resolver:
//
//   1. Initiate a server switch, by telling a connection to switch servers.
//      We always tell the connection to use ONE particular server.
//   2. Track sub-conn states and addresses through the custom balancer.
//		While the connection is switching from one sub-conn to another, it
//      calls into our custom balancer each tim a sub-conn changes states.
//	 3. Wait for the server switch to finish. Our balancer should see
//      the "new" sub-conn in READY status (on success) and any/all other sub-conns
//      in a SHUTDOWN status, since we always expect to connect to one server at a time.
//
import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

const balancerPolicyName = "consul-server-discovery"

type builder struct {
	balancer.Builder

	// this is only here so that the Watcher can call query the Balancer.
	// When a new Balancer is created, we set w.watcher.balancer.
	watcher *Watcher
}

func newBalancerBuilder(w *Watcher) balancer.Builder {
	return &builder{watcher: w}
}

func (b *builder) Build(cc balancer.ClientConn, opt balancer.BuildOptions) balancer.Balancer {
	// some way of hooking up the watcher to the balancer
	blr := &balancerWrapper{
		scs: map[balancer.SubConn]*subConnState{},
	}
	b.Builder = base.NewBalancerBuilder(balancerPolicyName, blr, base.Config{HealthCheck: false})
	blr.Balancer = b.Builder.Build(cc, opt)

	b.watcher.balancer = blr
	return b.watcher.balancer

}

func (*builder) Name() string {
	return balancerPolicyName
}

type balancerWrapper struct {
	balancer.Balancer

	// State to track sub-connections
	scs map[balancer.SubConn]*subConnState

	lock sync.Mutex
}

type subConnState struct {
	sc    balancer.SubConn
	state balancer.SubConnState
	addr  resolver.Address
}

// WaitForTransition waits for the load balancer to see that a connection has transitioned to
// the given server. It expects to see one sub-connection in Ready state for the given address,
// and all other sub-connectionsin Shutdown state.
//
// TODO: add context parameter
func (b *balancerWrapper) WaitForTransition(to ServerAddr) error {
	var targetConn balancer.SubConn

	// wait until we have the sub conns we are interested in.
	err := retryTimeout(context.TODO(), 250*time.Millisecond, 10*time.Second, func() error {
		b.lock.Lock()
		defer b.lock.Unlock()

		for sc, state := range b.scs {
			if state.addr.Addr == to.String() {
				targetConn = sc
				return nil
			}
		}

		return fmt.Errorf("missing sub conn for server change")
	})
	if err != nil {
		return err
	}

	log.Printf("transition: targetConn=%+v", targetConn)

	// wait until sub cons are in the righ state.
	err = retryTimeout(context.Background(), 250*time.Millisecond, 10*time.Second, func() error {
		b.lock.Lock()
		defer b.lock.Unlock()

		log.Printf("wait for transition to finish")
		for sc, state := range b.scs {
			// all other connections should be shutdown
			if sc != targetConn && state.state.ConnectivityState != connectivity.Shutdown {
				return fmt.Errorf("other sub conns not shutdown (state=%s)", state.state.ConnectivityState)
			}
			// our connection should be ready
			if sc == targetConn && state.state.ConnectivityState != connectivity.Ready {
				return fmt.Errorf("target sub conn not ready (state=%s)", state.state.ConnectivityState)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// stop tracking other sub conns.
	log.Printf("transition done")
	for sc := range b.scs {
		if sc != targetConn {
			delete(b.scs, sc)
		}
	}
	return nil
}

// UpdateSubConnState is called by gRPC when the state of a SubConn
// changes.
func (b *balancerWrapper) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	b.Balancer.UpdateSubConnState(sc, state)
	log.Printf("UpdateSubConnState, sc=%#v, state=%+v", sc, state)

	b.lock.Lock()
	defer b.lock.Unlock()

	// Store/update the sub-conn and its state.
	_, ok := b.scs[sc]
	if !ok {
		b.scs[sc] = &subConnState{sc: sc}
	}
	b.scs[sc].state = state
}

// Build implements base.PickerBuilder
//
// This is called when the set of ready sub-connections has changed.
func (b *balancerWrapper) Build(info base.PickerBuildInfo) balancer.Picker {
	b.lock.Lock()
	defer b.lock.Unlock()

	log.Printf("pickerBuilder.Build (%d subconns)", len(info.ReadySCs))
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	// Update the sub-conn address.
	for sc, i := range info.ReadySCs {
		conn, ok := b.scs[sc]
		if !ok {
			panic("picker.Build received ready connection it doesn't know about")
		}
		conn.addr = i.Address
	}

	return b
}

// Pick implements balancer.Picker
// This is called prior to each gRPC message to choose the sub-conn to use.
func (b *balancerWrapper) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	// Pick any READY address.
	for sc, conn := range b.scs {
		if conn.state.ConnectivityState == connectivity.Ready {
			log.Printf("pickerBuilder.Pick (have %d subconns, picking %s)", len(b.scs), conn.addr.Addr)
			return balancer.PickResult{SubConn: sc}, nil
		}
	}
	log.Printf("pickerBuilder.Pick failed! (have %d subconns, none ready)", len(b.scs))
	return balancer.PickResult{}, fmt.Errorf("no ready conns")
}
