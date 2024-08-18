package temp

import (
	"encoding/json"
	uuid2 "github.com/google/uuid"
	"log"
	"moss/pkg/setting"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func post(w http.ResponseWriter, r *http.Request) {
	output, _ := uuid2.NewRandom()
	uuid := strings.TrimSuffix(output.String(), "\n")
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	size, e := strconv.ParseInt(r.Header.Get("size"), 0, 64)
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	t := tempInfo{uuid, name, size}
	e = t.writeToFile()
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	create, err := os.Create(setting.STORAGE_ROOT + "/temp/" + t.Uuid + ".dat")
	if err != nil {
		log.Println("post create file error:", err)
		return
	}
	create.Close()
	w.Write([]byte(uuid))
}

func (t *tempInfo) writeToFile() error {
	f, e := os.Create(setting.STORAGE_ROOT + "/temp/" + t.Uuid)
	if e != nil {
		return e
	}
	defer f.Close()
	b, _ := json.Marshal(t)
	f.Write(b)
	return nil
}
