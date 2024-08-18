package object

import (
	"log"
	"moss/api_server/heartbeat"
	"moss/api_server/locate"
	"moss/pkg/es"
	"moss/pkg/rs"
	"moss/pkg/utils"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func post(w http.ResponseWriter, r *http.Request) {
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	size, err := strconv.ParseInt(r.Header.Get("size"), 0, 64)
	if err != nil {
		log.Println("post error:", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	hash := utils.GetHashFromHeader(r.Header)
	if hash == "" {
		log.Println("post error: hash is empty")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if locate.Exist(hash) {
		err = es.AddVersion(name, size, hash)
		if err != nil {
			log.Println("post error:", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		} else {
			w.WriteHeader(http.StatusOK)
			return
		}
	}
	ds := heartbeat.ChooseRandomDataServer(rs.ALL_SHARDS, nil)
	if len(ds) < rs.ALL_SHARDS {
		log.Println("post cannot find enough data server")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	stream, err := rs.NewRSResumablePutStream(ds, name, hash, size)
	if err != nil {
		log.Println("post error:", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("location", "/temp/"+url.PathEscape(stream.ToToken()))
	w.WriteHeader(http.StatusCreated)
}
