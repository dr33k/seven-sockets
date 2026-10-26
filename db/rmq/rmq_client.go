package rmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"seven.com/sockets/env"
)

type RabbitMQClient struct {
	Conn    *amqp091.Connection
	Channel *amqp091.Channel
}

type Event struct {
	Type      string `json:"event_type"`
	AcctID    string `json:"acct_id"`
	Payload   string `json:"payload"`
	Timestamp int64  `json:"timestamp"`
}

var RmqClient *RabbitMQClient

func InitRMQ(url string) error {
	const handlerName = "init-rmq"
	var err error

	log.Printf("[%s] Connecting to broker...", handlerName)
	conn, err := amqp091.Dial(url)
	if err != nil {
		return fmt.Errorf("[%s] Failed to connect to RabbitMQ: %w", handlerName, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("[%s] Failed to open a RabbitMQ channel: %w", handlerName, err)
	}

	RmqClient = &RabbitMQClient{
		Conn:    conn,
		Channel: ch,
	}

	log.Printf("[%s] Successfully connected and initialized channel and exchange", handlerName)
	return nil
}

func DeclareExchange(exchangeName string) error {
	const handlerName = "declare-rmq-exchange"

	//Main Exchange
	err := RmqClient.Channel.ExchangeDeclare(
		exchangeName, //name
		"topic",      //type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return fmt.Errorf("[%s] Failed to declare %s exchange: %w", handlerName, exchangeName, err)
	}

	//Dead Letter Exchange
	dlxExchangeName := exchangeName+"_dlx"

	err = RmqClient.Channel.ExchangeDeclare(
		dlxExchangeName, //name
		"direct",      //type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return fmt.Errorf("[%s] Failed to declare %s exchange: %w", handlerName, dlxExchangeName+"-dlx", err)
	}

	log.Printf("[%s] Exchanges %s and %s duccessfully declared", handlerName, exchangeName, dlxExchangeName)
	return nil
}

func DeclareQueue(queueName, exchangeName, routingKey string, qargs amqp091.Table) (*amqp091.Queue, error) {
	const handlerName = "declare-rmq-csm-queue"
	ch := RmqClient.Channel

	// 1. Declare a durable queue for your presence service
	q, err := ch.QueueDeclare(
		queueName, // queue name
		true,      // durable
		false,     // auto-delete when unused
		false,     // exclusive
		false,     // no-wait
		qargs,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("[%s] Failed to declare queue: %w", handlerName, err)
	}

	// 2. Bind the queue to your exchange using a routing key (or wildcard)
	err = ch.QueueBind(
		q.Name,       // queue name
		routingKey,   // routing key pattern (matches user.presence.heartbeat, etc.)
		exchangeName, // exchange name
		false,        // no-wait
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("[%s] Failed to bind queue to exchange: %w", handlerName, err)
	}

	return &q, nil
}

// StartConsumer starts a background worker goroutine to consume and persist presence events
func StartConsumer(ctx context.Context, q *amqp091.Queue, handler func(*Event) error) error {
	const handlerName =  "start-rmq-consumer"

	if handler == nil {
		return fmt.Errorf("[%s] Could not start consumer. No handler assigned", handlerName)
	}

	ch := RmqClient.Channel

	// Start consuming from the queue
	msgs, err := ch.Consume(
		q.Name,           // queue
		q.Name+"-worker", // consumer tag
		false,            // auto-ack (set to false so we manually Ack after DB save)
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
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
				//Launch another goroutine to process the message and retries
				go func(msg amqp091.Delivery){
					const workerName = "rmq-retry-worker"

					log.Printf("[%s] Received event: %s", handlerName, string(msg.Body))

					// 1. Unmarshal the event payload
					var ev Event
					if err := json.Unmarshal(d.Body, &ev); err != nil {
						log.Printf("[%s] Failed to unmarshal event: %v", handlerName, err)
						// Reject/nack the message without requeueing if it's malformed. Triggers DLQ
						_ = msg.Nack(false, false)
						return
					}

					// 2. Persist to PostgreSQL using service/repository layer
					err := handler(&ev)
					if err == nil{
						// Acknowledge successful processing
						_ = msg.Ack(false)
						log.Printf("[%s] Successfully persisted presence for acct %s", handlerName, ev.AcctID)
						return

					} else {
						// --- FAILURE HANDLING ---
						retryCount := getRetryCount(msg.Headers)
						log.Printf("[%s] Processing failed (Attempt %d/%d): %v", workerName, retryCount+1, env.RmqMaxRetries, err)

						if retryCount >= env.RmqMaxRetries {
							log.Printf("[%s] Max retries reached. Sending message to Dead Letter Queue (DLQ).", workerName)
							// Setting requeue to false triggers the x-dead-letter-exchange routing
							_ = d.Nack(false, false) 
							return
						}

						// Implement a brief backoff pause before retrying
						time.Sleep(2 * time.Second)

						// Increment retry count in headers and re-queue
						incrementAndRequeue(d, ch)
					}
				}(d)
			}
		}
	}()

	log.Printf("[%s] Background presence consumer started successfully", handlerName)
	return nil
}

func CloseRMQ() {
	const handlerName = "close-rmq"
	if RmqClient == nil {
		return
	}
	if RmqClient.Channel != nil {
		if err := RmqClient.Channel.Close(); err != nil {
			log.Printf("[%s] Error closing channel: %v", handlerName, err)
		}
	}
	if RmqClient.Conn != nil {
		if err := RmqClient.Conn.Close(); err != nil {
			log.Printf("[%s] Error closing connection: %v", handlerName, err)
		}
	}
	log.Printf("[%s] Connection and channel closed successfully", handlerName)
}


// Helper to read current retry count from AMQP headers
func getRetryCount(headers amqp091.Table) int {
	if headers == nil {
		return 0
	}
	if val, ok := headers["x-retry-count"]; ok {
		if count, ok := val.(int32); ok {
			return int(count)
		}
		if count, ok := val.(int); ok {
			return count
		}
	}
	return 0
}

// Helper to bump retry count and push back onto the queue
func incrementAndRequeue(msg amqp091.Delivery, ch *amqp091.Channel) {
	retryCount := getRetryCount(msg.Headers)
	
	if msg.Headers == nil {
		msg.Headers = amqp091.Table{}
	}
	msg.Headers["x-retry-count"] = retryCount + 1

	// Publish a new message back to the exchange with updated headers, 
	// then Ack the old one so it leaves the head of the queue.
	err := ch.PublishWithContext(
		context.Background(),
		env.RmqPresenceExName,
		msg.RoutingKey,
		false,
		false,
		amqp091.Publishing{
			ContentType: msg.ContentType,
			Body:        msg.Body,
			Headers:     msg.Headers,
		},
	)

	if err == nil {
		_ = msg.Ack(false) // Safely remove original failing message
	} else {
		// Fallback if republication fails: Nack with requeue
		_ = msg.Nack(false, true)
	}
}
