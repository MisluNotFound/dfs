package temp

import (
	"fmt"
	"log"
	"moss/pkg/setting"
	"net/http"
	"os"
	"strings"
)

func head(w http.ResponseWriter, r *http.Request) {
	uuid := strings.Split(r.URL.EscapedPath(), "/")[2]
	f, err := os.Stat(setting.STORAGE_ROOT + "/temp/" + uuid + ".dat")
	if err != nil {
		log.Println("temp.head() error:", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("content-length", fmt.Sprintf("%d", f.Size()))
}
