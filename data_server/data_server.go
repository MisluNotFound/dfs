package main

import (
	"log"
	"moss/data_server/heartbeat"
	"moss/data_server/locate"
	"moss/data_server/object"
	"moss/data_server/temp"
	"moss/pkg/setting"
	"net/http"
	"os"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("arguments should like ./data_server rootPath port")
	}
	rootPath, port := os.Args[1], os.Args[2]
	setting.STORAGE_ROOT = rootPath
	setting.Port = port
	locate.CollectObjects()
	go heartbeat.StartHeartBeat()
	go locate.StartLocate()
	http.HandleFunc("/objects/", object.Handler)
	http.HandleFunc("/temp/", temp.Handler)
	log.Printf("data server running at %s file path: %s\n", port, rootPath)
	log.Fatal(http.ListenAndServe(setting.Port, nil))
}
