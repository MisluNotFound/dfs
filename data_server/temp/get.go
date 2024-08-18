package temp

import (
	"io"
	"log"
	"moss/pkg/setting"
	"net/http"
	"os"
	"strings"
)

func get(w http.ResponseWriter, r *http.Request) {
	uuid := strings.Split(r.URL.EscapedPath(), "/")[2]
	f, err := os.Open(setting.STORAGE_ROOT + "/temp/" + uuid + ".dat")
	if err != nil {
		log.Println("temp.get() error:", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer f.Close()
	io.Copy(w, f)
}
