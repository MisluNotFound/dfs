package heartbeat

import (
	"log"
	"moss/pkg/rabbitmq"
	"moss/pkg/setting"
	"time"
)

// StartHeartBeat TODO set server address
func StartHeartBeat() {
	q := rabbitmq.New(setting.RabbitMQAddr)
	defer q.Close()

	// send
	for {
		err := q.Publish("apiServers", "localhost"+setting.Port)
		if err != nil {
			log.Printf("send heartbeat error: %v\n", err)
		}

		time.Sleep(time.Second * 5)
	}
}
