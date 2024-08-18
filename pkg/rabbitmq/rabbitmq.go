package rabbitmq

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
)

type RabbitMQ struct {
	channel  *amqp.Channel
	conn     *amqp.Connection
	Name     string
	exchange string
}

func New(addr string) *RabbitMQ {
	conn, err := amqp.Dial(addr)
	if err != nil {
		panic(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	q, err := ch.QueueDeclare(
		"",
		false,
		true,
		false,
		false,
		nil)
	if err != nil {
		panic(err)
	}
	mq := new(RabbitMQ)
	mq.channel = ch
	mq.conn = conn
	mq.Name = q.Name
	return mq
}

func (q *RabbitMQ) Bind(exchange string) {
	err := q.channel.QueueBind(
		q.Name,
		"",
		exchange,
		false,
		nil)
	if err != nil {
		log.Fatalf("mq %s bind error: %s", q.Name, err)
	}
	q.exchange = exchange
}

func (q *RabbitMQ) Send(queue string, body interface{}) error {
	str, err := json.Marshal(body)
	if err != nil {
		log.Printf("mq %s send marshal error: %s\n", q.Name, err)
		return err
	}
	err = q.channel.Publish("",
		queue,
		false,
		false,
		amqp.Publishing{
			ReplyTo: q.Name,
			Body:    []byte(str),
		})
	if err != nil {
		log.Printf("mq %s send publish error: %s\n", q.Name, err)
		return err
	}
	return nil
}

func (q *RabbitMQ) Publish(exchange string, body interface{}) error {
	str, err := json.Marshal(body)
	if err != nil {
		log.Printf("mq %s publish marshal error: %s\n", q.Name, err)
		return err
	}

	err = q.channel.Publish(exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ReplyTo: q.Name,
			Body:    []byte(str),
		})
	if err != nil {
		log.Printf("mq %s publish publish error: %s\n", q.Name, err)
		return err
	}
	return nil
}

func (q *RabbitMQ) Consume() <-chan amqp.Delivery {
	c, e := q.channel.Consume(q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if e != nil {
		panic(e)
	}
	return c
}

func (q *RabbitMQ) Close() {
	q.channel.Close()
	q.conn.Close()
}
