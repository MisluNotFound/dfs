package temp

import (
	"errors"
	"io"
	"log"
	"moss/api_server/locate"
	"moss/pkg/es"
	"moss/pkg/rs"
	"moss/pkg/utils"
	"net/http"
	"net/url"
	"strings"
)

func put(w http.ResponseWriter, r *http.Request) {
	token := strings.Split(r.URL.EscapedPath(), "/")[2]
	stream, err := rs.NewRSResumablePutStreamFromToken(token)
	if err != nil {
		log.Println("put error:", err)
		w.WriteHeader(http.StatusForbidden)
		return
	}
	current := stream.CurrentSize()
	if current == -1 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	offset := utils.GetOffsetFromHeader(r.Header)
	if offset != current {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	bytes := make([]byte, rs.BLOCK_SIZE)
	for {
		n, err := io.ReadFull(r.Body, bytes)
		if err != nil && err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
			log.Println("temp.put() error:", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		current += int64(n)
		if current > stream.Size {
			log.Println("temp.put() exceeds size limit: current:", current, "limit:", stream.Size)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		// avoid error caused by incorrect size
		if n != rs.BLOCK_SIZE && current != stream.Size {
			return
		}
		stream.Write(bytes[:n])
		// upload completely
		if current == stream.Size {
			log.Printf("temp.put() upload %d and size is %d", current, stream.Size)
			stream.Flush()
			getStream, _ := rs.NewRSResumableGetStream(stream.Servers, stream.Uuids, stream.Size)
			hash := url.PathEscape(utils.CalculateHash(getStream))
			if hash != stream.Hash {
				stream.Commit(false)
				log.Println("temp.put() put done but hash mismatch. calculated:", hash, "require:", stream.Hash)
				w.WriteHeader(http.StatusForbidden)
				return
			}
			if locate.Exist(url.PathEscape(hash)) {
				stream.Commit(false)
			} else {
				stream.Commit(true)
			}
			err = es.AddVersion(stream.Name, stream.Size, stream.Hash)
			if err != nil {
				log.Println("temp.put() error:", err)
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
	}
}
