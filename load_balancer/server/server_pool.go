package server

import (
	"log"
	"net/url"
	"sync/atomic"
)

type ServerPool struct {
	backends []*Backend
	current  uint64
}

func (s *ServerPool) NextIndex() int {
	s.current += 1
	return int(s.current % uint64(len(s.backends)))
}

func (s *ServerPool) GetNextPeer() *Backend {
	next := s.NextIndex()
	l := len(s.backends) + next
	for i := next; i < l; i++ {
		idx := i % len(s.backends)
		if s.backends[idx].IsAlive() {
			if i != next {
				atomic.StoreUint64(&s.current, uint64(idx))
			}
			return s.backends[idx]
		}
	}
	return nil
}

func (s *ServerPool) HealthCheck() {
	for _, b := range s.backends {
		status := "alive"
		alive := isBackendAlive(b.Url)
		b.SetAlive(alive)
		if !alive {
			status = "dead"
		}
		log.Printf("HealthCheck() %s is [%s] \n", b.Url, status)
	}
}

func (s *ServerPool) MarkBackendStatus(backend *url.URL, status bool) {
	for _, b := range s.backends {
		if b.Url.String() == backend.String() {
			b.SetAlive(status)
			break
		}
	}
}

func (s *ServerPool) AddBackend(backend *Backend) {
	s.backends = append(s.backends, backend)
}
