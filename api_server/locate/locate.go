package locate

import (
	"encoding/json"
	"log"
	"moss/pkg/rabbitmq"
	"moss/pkg/rs"
	"moss/pkg/setting"
	"moss/pkg/types"
	"time"
)

func Exist(name string) bool {
	return len(Locate(name)) >= rs.ALL_SHARDS
}

// Locate detect which server has data shards
// It publishes name to data servers
// It returns id:server
func Locate(name string) (locateInfo map[int]string) {
	q := rabbitmq.New(setting.RabbitMQAddr)
	log.Println("Locate param 'name': ", name)
	q.Publish("dataServers", name)
	c := q.Consume()
	locateInfo = make(map[int]string)
	go func() {
		time.Sleep(time.Second)
		q.Close()
	}()
	for i := 0; i < rs.ALL_SHARDS; i++ {
		msg := <-c
		if len(msg.Body) == 0 {
			return
		}
		var info types.LocateMessage
		json.Unmarshal(msg.Body, &info)
		locateInfo[info.Id] = info.Addr
	}
	return
}
