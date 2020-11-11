package process

import (
	"cdc-distribute/conf"
	"encoding/json"
	"fmt"
	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/streadway/amqp"
)

type rabbitProcess struct {
	client     *rabbitmq.Connection
	rabbitConf *conf.RabbitConf
}

// newRabbitProcess create handler write data to rabbit
func newRabbitProcess(rabbitConf *conf.RabbitConf) Process {
	if rabbitConf == nil {
		panic("rabbit conf is nil")
	}

	if rabbitConf.Conn == "" {
		panic("Cannot initialize connection to broker, connectionString not set. Have you initialized?")
	}

	var err error
	conn, err := rabbitmq.Dial(fmt.Sprintf("%s/", rabbitConf.Conn))
	if err != nil {
		panic("Failed to connect to AMQP compatible broker at: " + rabbitConf.Conn + " ERR:" + err.Error())
	}

	process := &rabbitProcess{
		client:     conn,
		rabbitConf: rabbitConf,
	}

	return process
}

func (m *rabbitProcess) Write(wal ...*interface{}) error {

	if m.client == nil {
		panic("Tried to send message before connection was initialized. Don't do that.")
	}
	ch, err := m.client.Channel() // Get a channel from the connection

	if err != nil {
		panic("Failed create Channel. ERR:" + err.Error())
	}
	defer ch.Close()

	queue, err := ch.QueueDeclare( // Declare a queue that will be created if not exists with some args
		m.rabbitConf.Queue, // our queue name
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		panic("Failed create queue. ERR:" + err.Error())
	}

	bytes, _ := json.Marshal(wal)

	// Publishes a message onto the queue.
	err = ch.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        bytes, // Our JSON body as []byte
		})
	fmt.Printf("A message was sent to queue %v: %v", m.rabbitConf.Queue, bytes)
	return err

}

func (e *rabbitProcess) Close() {

}
