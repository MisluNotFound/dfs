package temp

import (
	"compress/gzip"
	"io"
	"moss/data_server/locate"
	"moss/pkg/setting"
	"moss/pkg/utils"
	"net/url"
	"os"
)

// commitTempObject rename dataFile to STORAGE_ROOT/objects/<hash>.X.<hash of shard X>
func commitTempObject(datFile string, tempinfo *tempInfo) {
	f, _ := os.Open(datFile)
	defer f.Close()
	hash := url.PathEscape(utils.CalculateHash(f))
	f.Seek(0, io.SeekStart)
	w, _ := os.Create(setting.STORAGE_ROOT + "/objects/" + tempinfo.Hash + "." + hash)
	w2 := gzip.NewWriter(w)
	io.Copy(w2, f)
	w2.Close()
	os.Remove(datFile)
	locate.Add(tempinfo.hash(), tempinfo.id())
}
