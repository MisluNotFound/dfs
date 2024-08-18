package main

import (
	"log"
	"moss/api_server/object"
	"moss/pkg/es"
	"moss/pkg/setting"
	"moss/pkg/utils"
	"path/filepath"
	"strings"
)

func main() {
	files, _ := filepath.Glob(setting.STORAGE_ROOT + "/objects/*")
	for i := range files {
		hash := strings.Split(filepath.Base(files[i]), ".")[0]
		verify(hash)
	}
}

func verify(hash string) {
	log.Println("verify", hash)
	size, e := es.SearchHashSize(hash)
	if e != nil {
		log.Println(e)
		return
	}
	stream, err := object.GetStream(hash, size)
	if err != nil {
		log.Println(err)
		return
	}
	d := utils.CalculateHash(stream)
	if d != hash {
		log.Println("object hash mismatch, calculated=%s hash=%s", d, hash)
	}
	stream.Close()
}
