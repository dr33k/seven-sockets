package rmq

import (
	"encoding/json"
	"fmt"

	"github.com/rabbitmq/amqp091-go"
)

func PubEvent(exchange, routingKey string, event Event) error{
	handlerName:="pub-event"
	if RmqClient == nil || RmqClient.Channel == nil{
		return fmt.Errorf("[%s] RabbitMQ client not initialized", handlerName)
	}

	bytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("[%s] Could not marshal event: %v", handlerName, err)
	}

	err = RmqClient.Channel.Publish(
		exchange,
		routingKey,
		false,
		false,
		amqp091.Publishing{
			ContentType: "application/json",
			Body: bytes,
		},
	)
	return err
}