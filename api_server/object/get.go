package object

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"moss/pkg/es"
	"moss/pkg/utils"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// GET /objects/<object_name>?version=<version_id>
func get(w http.ResponseWriter, r *http.Request) {
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	versionId := r.URL.Query()["version"]
	version := 0
	var err error
	if len(versionId) != 0 {
		version, err = strconv.Atoi(versionId[0])
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	metadata, err := es.GetMetadata(name, version)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if metadata.Hash == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	hash := url.PathEscape(metadata.Hash)
	stream, err := GetStream(hash, metadata.Size)
	if err != nil {
		log.Println("get get stream error:", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	offset := utils.GetOffsetFromHeader(r.Header)
	if offset != 0 {
		stream.Seek(offset, io.SeekCurrent)
		w.Header().Set("content-range", fmt.Sprintf("bytes="))
		w.WriteHeader(http.StatusPartialContent)
	}
	acceptGzip := false
	encoding := r.Header["Accept-Encoding"]
	for i := range encoding {
		if encoding[i] == "gzip" {
			acceptGzip = true
			break
		}
	}
	if acceptGzip {
		w.Header().Set("content-encoding", "gzip")
		w2 := gzip.NewWriter(w)
		io.Copy(w2, stream)
		w2.Close()
	} else {
		_, err = io.Copy(w, stream)
		if err != nil {
			log.Println("get copy error:", err)
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}
	stream.Close()
}
