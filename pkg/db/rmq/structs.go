package rmq

import "github.com/rabbitmq/amqp091-go"

type RabbitMQClient struct {
	Conn    *amqp091.Connection
	Channel *amqp091.Channel
}

type Event struct {
	Type      string `json:"event_type"`
	AcctID    string `json:"acct_id"`
	Payload   any `json:"payload"`
	Timestamp int64  `json:"timestamp"`
}
