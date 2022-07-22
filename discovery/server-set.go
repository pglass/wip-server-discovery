package discovery

import (
	"fmt"
	"sync"
)

type ServerIPs []string
type ServerStatus bool

const (
	// We don't really know if they are healthy or unhealthy.
	Healthy   ServerStatus = true
	Unhealthy              = false
)

// I guess this is just a concurrent map
// We expect a small number of server addrs, so this is probably fine.
// It includes a bit of logic to automatically set servers healthy or not.
type ServerSet struct {
	lock  sync.Mutex
	addrs map[string]ServerStatus
}

func NewServerSet() *ServerSet {
	return &ServerSet{
		addrs: map[string]ServerStatus{},
	}
}

// SetKnownHealthy sets the given addrs as healthy and all other addrs as unhealthy.
func (s *ServerSet) SetKnownHealthy(addrs ...string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// set all addrs to unhealthy
	for a := range s.addrs {
		s.addrs[a] = Unhealthy
	}
	// set the given addrs to healthy
	for _, a := range addrs {
		s.addrs[a] = Healthy
	}
}

// Set marks the addr as the given status.
func (s *ServerSet) Set(addr string, status ServerStatus) {
	s.addrs[addr] = status
}

// Get returns all addresses matching the given status.
func (s *ServerSet) Get(status ServerStatus) ServerIPs {
	s.lock.Lock()
	defer s.lock.Unlock()

	result := []string{}
	for addr, s := range s.addrs {
		if s == status {
			result = append(result, addr)
		}
	}
	return result
}

func (s *ServerSet) String() string {
	s.lock.Lock()
	defer s.lock.Unlock()

	return fmt.Sprint(s.addrs)
}
