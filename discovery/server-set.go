package discovery

import (
	"fmt"
	"sync"
)

type ServerStatus bool

type ServerState struct {
	Status   ServerStatus
	Features map[string]bool
	Addr     ServerAddr
}

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
