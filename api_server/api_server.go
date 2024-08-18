package main

import (
	"log"
	"moss/api_server/heartbeat"
	"moss/api_server/locate"
	"moss/api_server/object"
	"moss/api_server/temp"
	"moss/api_server/versions"
	"net/http"
)

func main() {
	go heartbeat.ListenHeartbeat()
	http.HandleFunc("/objects/", object.Handler)
	http.HandleFunc("/locate/", locate.Handler)
	http.HandleFunc("/temp/", temp.Handler)
	http.HandleFunc("/versions/", versions.Handler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
