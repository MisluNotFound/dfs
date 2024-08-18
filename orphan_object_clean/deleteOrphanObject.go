package main

import (
	"log"
	"moss/pkg/es"
	"moss/pkg/setting"
	"net/http"
	"path/filepath"
	"strings"
)

func main() {
	files, _ := filepath.Glob(setting.STORAGE_ROOT + "/objects/*")
	for i := range files {
		hash := strings.Split(filepath.Base(files[i]), ".")[0]
		hashInMetadata, err := es.HasHash(hash)
		if err != nil {
			log.Println(err)
			return
		}
		if !hashInMetadata {
			del(hash)
		}
	}
}

func del(hash string) {
	log.Println("delete", hash)
	url := "http://127.0.0.1" + setting.Port + "/objects/" + hash
	request, _ := http.NewRequest("DELETE", url, nil)
	client := http.Client{}
	client.Do(request)
}
