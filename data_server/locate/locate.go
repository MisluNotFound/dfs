package locate

import (
	"log"
	"moss/pkg/rabbitmq"
	"moss/pkg/setting"
	"moss/pkg/types"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var objects sync.Map

func Locate(hash string) int {
	log.Println("searching hash: ", hash)
	id, ok := objects.Load(hash)
	if !ok {
		return -1
	}
	return id.(int)
}

func Add(hash string, id int) {
	objects.Store(hash, id)
}

func Del(hash string) {
	objects.Delete(hash)
}

func StartLocate() {
	q := rabbitmq.New(setting.RabbitMQAddr)
	defer q.Close()
	q.Bind("dataServers")
	c := q.Consume()
	for msg := range c {
		hash, err := strconv.Unquote(string(msg.Body))
		if err != nil {
			log.Fatalf("StartLocate unquote msg error: %s\n", err)
		}

		if id := Locate(hash); id != -1 {
			q.Send(msg.ReplyTo, types.LocateMessage{
				Addr: setting.Port,
				Id:   id,
			})
		}
	}
}

func CollectObjects() {
	files, err := filepath.Glob(setting.STORAGE_ROOT + "\\objects\\*")
	log.Println(setting.STORAGE_ROOT + "\\objects\\*")
	if err != nil {
		log.Println(err)
	}

	for i := range files {
		f := strings.Split(filepath.Base(files[i]), ".")
		if len(f) < 3 {
			log.Panic(files[i])
		}
		hash := f[0]
		id, err := strconv.Atoi(f[1])
		if err != nil {
			log.Panic(err)
		}
		log.Println("collect hash:", hash, "id:", id)
		objects.Store(hash, id)
	}
}
