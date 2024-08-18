package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	limiter2 "moss/load_balancer/limiter"
	"moss/load_balancer/server"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

const (
	Attempts = iota
	Retry
)

var serverPool server.ServerPool
var limiter *limiter2.Limiter

func lb(w http.ResponseWriter, r *http.Request) {
	if !limiter.GetToken() {
		time.Sleep(time.Millisecond * 50)
		if !limiter.GetToken() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
	}
	attempts := GetAttemptsFromContext(r)
	if attempts > 3 {
		log.Printf("%s(%s) max attemp reached\n", r.RemoteAddr, r.URL.Path)
		http.Error(w, "service not available", http.StatusServiceUnavailable)
		return
	}
	peer := serverPool.GetNextPeer()
	if peer != nil {
		peer.ReverseProxy.ServeHTTP(w, r)
	}
	http.Error(w, "Service not available", http.StatusServiceUnavailable)
}

func GetRetryFromContext(r *http.Request) int {
	if retry, ok := r.Context().Value(Retry).(int); ok {
		return retry
	}
	return 1
}

func GetAttemptsFromContext(r *http.Request) int {
	if attempts, ok := r.Context().Value(Attempts).(int); ok {
		return attempts
	}
	return 1
}

func healthCheck() {
	t := time.NewTicker(time.Second * 20)
	for {
		select {
		case <-t.C:
			log.Println("start health check")
			serverPool.HealthCheck()
			log.Println("health check completed")
		}
	}

}

func main() {
	var serverList string
	var port int
	flag.StringVar(&serverList, "backends", "", "Load balance backends, use commas to separate")
	flag.IntVar(&port, "port", 3030, "Port to server")
	flag.Parse()

	if len(serverList) == 0 {
		log.Fatal("no backends provided")
	}
	limiter = limiter2.NewLimiter(time.Millisecond*10, 500)
	tokens := strings.Split(serverList, ",")
	for _, token := range tokens {
		serverUrl, err := url.Parse(token)
		if err != nil {
			log.Fatal(err)
		}
		proxy := httputil.NewSingleHostReverseProxy(serverUrl)
		proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, e error) {
			log.Printf("server %s error: %s\n", serverUrl.Host, e.Error())
			retries := GetRetryFromContext(request)
			if retries < 3 {
				select {
				case <-time.After(time.Millisecond * 10):
					ctx := context.WithValue(request.Context(), "retry", retries+1)
					proxy.ServeHTTP(writer, request.WithContext(ctx))
				}
				return
			}

			serverPool.MarkBackendStatus(serverUrl, false)
			attempts := GetAttemptsFromContext(request)
			log.Printf("%s(%s) attempting retry %d\n", request.RemoteAddr, request.URL.Path, attempts)
			ctx := context.WithValue(request.Context(), "retry", attempts+1)
			lb(writer, request.WithContext(ctx))
		}

		serverPool.AddBackend(&server.Backend{
			Url:          serverUrl,
			Alive:        true,
			ReverseProxy: proxy,
		})
		log.Printf("configured server %s\n", serverUrl)
	}

	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(lb),
	}

	go healthCheck()
	log.Println("load balancer started")
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
