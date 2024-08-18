package rs

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"moss/pkg/objectstream"
	"moss/pkg/utils"
	"net/http"
)

type resumableToken struct {
	Name    string
	Hash    string
	Size    int64
	Servers []string
	Uuids   []string
}

type RSResumablePutStream struct {
	*resumableToken
	*RSPutStream
}

func NewRSResumablePutStream(servers []string, name, hash string, size int64) (*RSResumablePutStream, error) {
	putstream, err := NewRSPutStream(servers, hash, size)
	if err != nil {
		return nil, err
	}
	uuids := make([]string, ALL_SHARDS)
	for i := range uuids {
		uuids[i] = putstream.writers[i].(*objectstream.TempPutStream).Uuid
	}
	token := &resumableToken{name, hash, size, servers, uuids}
	return &RSResumablePutStream{token, putstream}, nil
}

// ToToken return base64 encoded s's json string
func (s *RSResumablePutStream) ToToken() string {
	b, _ := json.Marshal(s)
	return base64.StdEncoding.EncodeToString(b)
}

func NewRSResumablePutStreamFromToken(token string) (*RSResumablePutStream, error) {
	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}
	rToken := &resumableToken{}
	json.Unmarshal(b, rToken)
	writers := make([]io.Writer, ALL_SHARDS)
	for i := range writers {
		writers[i] = &objectstream.TempPutStream{Server: rToken.Servers[i], Uuid: rToken.Uuids[i]}
	}
	enc := NewEncoder(writers)
	return &RSResumablePutStream{rToken, &RSPutStream{enc}}, nil
}

// CurrentSize get upload size from data server
func (s *RSResumablePutStream) CurrentSize() int64 {
	url := fmt.Sprintf("http://%s/temp/%s", s.Servers[0], s.Uuids[0])
	log.Println("current size:", url)
	r, err := http.Head(url)
	if err != nil {
		log.Println("CurrentSize error:", err)
		return -1
	}
	if r.StatusCode != http.StatusOK {
		log.Println("CurrentSize error: server returns code:", r.StatusCode)
		return -1
	}
	size := utils.GetSizeFromHeader(r.Header) * DATA_SHARDS
	if size > s.Size {
		size = s.Size
	}
	return size
}
