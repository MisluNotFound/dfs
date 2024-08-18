package temp

import (
	"fmt"
	"log"
	"moss/pkg/rs"
	"net/http"
	"strings"
)

// head sets uploaded size to content-length
func head(w http.ResponseWriter, r *http.Request) {
	token := strings.Split(r.URL.String(), "/")[2]
	stream, err := rs.NewRSResumablePutStreamFromToken(token)
	if err != nil {
		log.Println("temp.head() error:", err)
		w.WriteHeader(http.StatusForbidden)
		return
	}
	current := stream.CurrentSize()
	if current == -1 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("content-length", fmt.Sprintf("%d", current))
}
