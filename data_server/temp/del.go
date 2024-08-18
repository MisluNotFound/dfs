package temp

import (
	"moss/pkg/setting"
	"net/http"
	"os"
	"strings"
)

// del remove object temp info and data file
func del(w http.ResponseWriter, r *http.Request) {
	uuid := strings.Split(r.URL.EscapedPath(), "/")[2]
	infoFile := setting.STORAGE_ROOT + "/temp/" + uuid
	dataFile := infoFile + ".dat"
	os.Remove(infoFile)
	os.Remove(dataFile)
}
