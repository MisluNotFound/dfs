// Package setting TODO use config file instead
package setting

const (
	RabbitMQAddr = "amqp://guest:guest@localhost:5672/"
	ES           = "http://127.0.0.1:9200"
)

var (
	STORAGE_ROOT string
	Port         string
)
