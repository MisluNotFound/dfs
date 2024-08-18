package objectstream

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

type TempPutStream struct {
	Server string
	Uuid   string
}

func NewTempPutStream(server, object string, size int64) (*TempPutStream, error) {
	request, err := http.NewRequest("POST", "http://"+server+"/temp/"+object, nil)
	log.Printf("NewTempPutStream server: %s request: %s \n", server, request.URL)
	if err != nil {
		return nil, err
	}

	request.Header.Set("size", fmt.Sprintf("%d", size))
	client := http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}

	uuid, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	log.Println("NewPutStream returns uuid: ", string(uuid))
	return &TempPutStream{server, string(uuid)}, nil
}

func (w *TempPutStream) Write(p []byte) (int, error) {
	request, err := http.NewRequest("PATCH", "http://"+w.Server+"/temp/"+w.Uuid, strings.NewReader(string(p)))
	if err != nil {
		return 0, err
	}
	client := http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return 0, err
	}

	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("dataServer return http code: %d", response.StatusCode)
	}
	return len(p), nil
}

// Commit delete or put the temp object depends on whether the
// hash equals to data server calculated or not.
func (w *TempPutStream) Commit(good bool) {
	method := "DELETE"
	if good {
		method = "PUT"
	}
	request, _ := http.NewRequest(method, "http://"+w.Server+"/temp/"+w.Uuid, nil)
	client := http.Client{}
	_, err := client.Do(request)
	if err != nil {
		log.Printf("commit method: %s error: %s \n", method, err)
		return
	}
}

func NewTempGetStream(server, uuid string) (*GetStream, error) {
	return newGetStream("http://" + server + "/temp/" + uuid)
}
