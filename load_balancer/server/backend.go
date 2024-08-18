package server

import (
	"log"
	"net"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"
)

type Backend struct {
	Url          *url.URL
	Alive        bool
	mutex        sync.RWMutex
	ReverseProxy *httputil.ReverseProxy
}

func (b *Backend) SetAlive(alive bool) {
	b.mutex.Lock()
	b.Alive = alive
	b.mutex.Unlock()
}

func (b *Backend) IsAlive() bool {
	b.mutex.RLock()
	alive := b.Alive
	b.mutex.RUnlock()
	return alive
}

func isBackendAlive(url *url.URL) bool {
	timeout := 2 * time.Second
	conn, err := net.DialTimeout("tcp", url.Host, timeout)
	if err != nil {
		log.Println("Backend unreachable:", err)
		return false
	}
	_ = conn.Close()
	return true
}
