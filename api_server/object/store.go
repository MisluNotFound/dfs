package object

import (
	"fmt"
	"io"
	"log"
	"moss/api_server/locate"
	"moss/pkg/utils"
	"net/http"
	"net/url"
)

// storeObject
func storeObject(r io.Reader, hash string, size int64) (int, error) {
	path := url.PathEscape(hash)
	if locate.Exist(path) {
		return http.StatusOK, nil
	}
	stream, err := putStream(path, size)
	if err != nil {
		log.Printf("storeObject putStream err: %s\n", err)
		return http.StatusInternalServerError, err
	}
	reader := io.TeeReader(r, stream)
	d := utils.CalculateHash(reader)
	if d != hash {
		stream.Commit(false)
		return http.StatusBadRequest, fmt.Errorf("object hash mismatch, calculated %s, expected %s", d, hash)
	}
	stream.Commit(true)
	return http.StatusOK, nil
}
