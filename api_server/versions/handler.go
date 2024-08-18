package versions

import (
	"encoding/json"
	"fmt"
	"log"
	"moss/pkg/es"
	"net/http"
	"strings"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	from, size := 0, 1000
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	fmt.Println(r.URL, name)
	for {
		metadata, err := es.SearchAllVersions(name, from, size)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		for i := range metadata {
			b, _ := json.Marshal(metadata[i])
			w.Write(b)
			w.Write([]byte("\n"))
		}
		if len(metadata) != size {
			return
		}
		from += size
	}
}
