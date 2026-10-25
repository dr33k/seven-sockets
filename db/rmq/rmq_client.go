package rmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/rabbitmq/amqp091-go"
)


type RabbitMQClient struct{
	Conn *amqp091.Connection
	Channel *amqp091.Channel
}

type Event struct {
	Type string `json:"event_type"`
	AcctID    string `json:"acct_id"`
	Payload   string `json:"payload"`
	Timestamp int64  `json:"timestamp"`
}


var RmqClient *RabbitMQClient

func InitRMQ(url string) error{
	handlerName := "init-rmq"
	var err error

	log.Printf("[%s] Connecting to broker...", handlerName)
	conn, err := amqp091.Dial(url)
	if err!= nil{
		return fmt.Errorf("[%s] Failed to connect to RabbitMQ: %w", handlerName, err)
	}

	ch, err:= conn.Channel()
	if err!= nil{
		conn.Close()
		return fmt.Errorf("[%s] Failed to open a RabbitMQ channel: %w", handlerName, err)
	}

	RmqClient = &RabbitMQClient{
		Conn: conn,
		Channel: ch,
	}

	log.Printf("[%s] Successfully connected and initialized channel and exchange", handlerName)
	return nil
}

func DeclareRmqExchange(exchangeName string)error{
	handlerName := "declare-rmq-exchange"
	err := RmqClient.Channel.ExchangeDeclare(
		exchangeName, //name
		"topic",				//type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	if err != nil{
		return fmt.Errorf("[%s] Failed to declare exchange: %w", handlerName, err)
	}
	return nil
}

func DeclareRmqConsumerQueue(queueName, exchangeName, routingKey string) (*amqp091.Queue, error) {
	handlerName := "declare-rmq-csm-queue"
	ch := RmqClient.Channel

	// 1. Declare a durable queue for your presence service
	q, err := ch.QueueDeclare(
		queueName, 							// queue name
		true,                         // durable
		false,                        // auto-delete when unused
		false,                        // exclusive
		false,                        // no-wait
		nil,                          // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("[%s] Failed to declare queue: %w", handlerName, err)
	}

	// 2. Bind the queue to your exchange using a routing key (or wildcard)
	err = ch.QueueBind(
		q.Name,                	// queue name
		routingKey,     		// routing key pattern (matches user.presence.heartbeat, etc.)
		exchangeName,   		// exchange name
		false,                 	// no-wait
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("[%s] Failed to bind queue to exchange: %w", handlerName, err)
	}

	return &q, nil
}

// StartRmqConsumer starts a background worker goroutine to consume and persist presence events
func StartRmqConsumer(ctx context.Context, q *amqp091.Queue, handler func(*Event) error) error {
	handlerName := "start-rmq-consumer"
	ch := RmqClient.Channel

	// Start consuming from the queue
	msgs, err := ch.Consume(
		q.Name, // queue
		q.Name+"-worker",          // consumer tag
		false,                        // auto-ack (set to false so we manually Ack after DB save)
		false,                        // exclusive
		false,                        // no-local
		false,                        // no-wait
		nil,                          // args
	)
	if err != nil {
		return fmt.Errorf("[%s] Failed to register a consumer: %w", handlerName, err)
	}

	// Run the consumer loop in a background goroutine
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Printf("[%s] Shutting down presence consumer...", handlerName)
				return
			case d, ok := <-msgs:
				if !ok {
					log.Printf("[%s] Message channel closed.", handlerName)
					return
				}

				log.Printf("[%s] Received event: %s", handlerName, string(d.Body))

				// 1. Unmarshal the event payload
				var ev Event // Your event struct
				if err := json.Unmarshal(d.Body, &ev); err != nil {
					log.Printf("[%s] Failed to unmarshal event: %v",handlerName, err)
					// Reject/nack the message without requeueing if it's malformed
					_ = d.Nack(false, false)
					continue
				}

				// 2. Persist to PostgreSQL using your service/repository layer
				if err := handler(&ev); err != nil {
					log.Printf("[%s] Failed to persist presence for acct %s: %v", handlerName , ev.AcctID, err)
					// Nack and requeue so it retries later
					_ = d.Nack(false, true)
					continue
				}

				// 3. Acknowledge successful processing
				_ = d.Ack(false)
				log.Printf("[%s] Successfully persisted presence for acct %s", handlerName, ev.AcctID)
			}
		}
	}()

	log.Printf("[%s] Background presence consumer started successfully", handlerName)
	return nil
}

func CloseRMQ(){
	handlerName := "close-rmq"
	if RmqClient == nil{
		return
	}
	if RmqClient.Channel != nil{
		if err := RmqClient.Channel.Close();err!= nil{
			log.Printf("[%s] Error closing channel: %v", handlerName, err)
		}
	}
	if RmqClient.Conn != nil{
		if err := RmqClient.Conn.Close(); err != nil{
			log.Printf("[%s] Error closing connection: %v", handlerName, err)
		}
	}
	log.Printf("[%s] Connection and channel closed successfully", handlerName)
}