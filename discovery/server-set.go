package discovery

import (
	"fmt"
	"net"
	"net/netip"
	"sync"
)

type ServerStatus bool

type ServerAddr struct {
	// AdddPort is useful because it can be compared, used as a map key,
	// and has an IsValid() method. The only downside seems to be that
	// it prints addresses in an ipv6 format (like [::ffff:172.21.0.2]:8502)
	netip.AddrPort
}

func MakeServerAddr(addr net.IPAddr, port int) ServerAddr {
	tcp := &net.TCPAddr{
		IP:   addr.IP,
		Port: port,
		Zone: addr.Zone,
	}
	return ServerAddr{tcp.AddrPort()}
}

func MakeServerAddrStr(addr string, port int) (ServerAddr, error) {
	a, err := netip.ParseAddr(addr)
	if err != nil {
		return ServerAddr{}, err
	}
	return ServerAddr{
		netip.AddrPortFrom(a, uint16(port)),
	}, nil
}

type ServerState struct {
	Status ServerStatus
	Addr   ServerAddr
}

const (
	// Maybe needs a rename. We don't know that the server is healthy or not.
	// "Healthy" really means "should we try to connect to this server?" and
	// "Unhealthy" means "don't try connecting to this server.
	Healthy   ServerStatus = true
	Unhealthy              = false
)

// I guess this is just a concurrent map
// We expect a small number of server addrs, so this is probably fine.
// It includes a bit of logic to automatically set servers healthy or not.
type ServerSet struct {
	lock  sync.Mutex
	addrs map[ServerAddr]ServerState
}

func NewServerSet() *ServerSet {
	return &ServerSet{
		addrs: map[ServerAddr]ServerState{},
	}
}

// SetKnownHealthy sets the given addrs as healthy and all other addrs as unhealthy.
func (s *ServerSet) SetKnownHealthy(addrs ...ServerAddr) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// set all addrs to unhealthy
	for a, state := range s.addrs {
		state.Status = Unhealthy
		s.addrs[a] = state
	}
	// set the given addrs to healthy
	for _, a := range addrs {
		state := s.addrs[a]
		state.Addr = a
		state.Status = Healthy
		s.addrs[a] = state
	}
}

// Set marks the addr as the given status.
func (s *ServerSet) Update(state ServerState) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.addrs[state.Addr] = state
}

// Get returns all addresses matching the given status.
func (s *ServerSet) Get(status ServerStatus) []ServerState {
	s.lock.Lock()
	defer s.lock.Unlock()

	result := []ServerState{}
	for _, state := range s.addrs {
		if state.Status == status {
			result = append(result, state)
		}
	}
	return result
}

func (s *ServerSet) String() string {
	s.lock.Lock()
	defer s.lock.Unlock()

	return fmt.Sprint(s.addrs)
}
