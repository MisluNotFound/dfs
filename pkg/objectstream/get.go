package objectstream

import (
	"fmt"
	"io"
	"log"
	"net/http"
)

type GetStream struct {
	reader io.Reader
}

// newGetStream send a Get request to data server
func newGetStream(url string) (*GetStream, error) {
	r, err := http.Get(url)
	if err != nil {
		log.Printf("newGetStream http.Get err:%v\n", err)
		return nil, err
	}

	if r.StatusCode != http.StatusOK {
		log.Printf("newGetStream http.Get return code %d\n", r.StatusCode)
		return nil, fmt.Errorf("dataServer return http code %d", r.StatusCode)
	}
	return &GetStream{reader: r.Body}, nil
}

func NewGetStream(server, object string) (*GetStream, error) {
	if server == "" || object == "" {
		return nil, fmt.Errorf("server or object is empty")
	}

	return newGetStream("http://127.0.0.1" + server + "/objects/" + object)
}

func (r *GetStream) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}
