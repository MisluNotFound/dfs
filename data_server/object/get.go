package object

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"log"
	"moss/data_server/locate"
	"moss/pkg/setting"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func get(w http.ResponseWriter, r *http.Request) {
	file := getFile(strings.Split(r.URL.EscapedPath(), "/")[2])
	if file == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	sendFile(w, file)
}

// getFile checks whether hash is legal and returns path
func getFile(name string) string {
	files, _ := filepath.Glob(setting.STORAGE_ROOT + "/objects/" + name + ".*")
	if len(files) != 1 {
		return ""
	}
	file := files[0]
	h := sha256.New()
	sendFile(h, file)
	d := url.PathEscape(base64.StdEncoding.EncodeToString(h.Sum(nil)))
	hash := strings.Split(file, ".")[2]
	if d != hash {
		log.Println("getFile object mismatch hash, remove", file)
		locate.Del(hash)
		os.Remove(file)
		return ""
	}
	return file
}

func sendFile(w io.Writer, file string) {
	f, err := os.Open(file)
	if err != nil {
		log.Println("object.sendFile() error:", err)
		return
	}
	defer f.Close()
	gzipReader, err := gzip.NewReader(f)
	if err != nil {
		log.Println("object.sendFile() error:", err)
		return
	}
	io.Copy(w, gzipReader)
	gzipReader.Close()
}
