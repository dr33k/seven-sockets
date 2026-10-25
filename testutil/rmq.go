package testutil

import (
	"context"
	"fmt"
	"testing"
	"time"

	"seven.com/sockets/db/rmq"
)

// ReadRabbitMQMessage pulls a single message synchronously from a queue with a timeout
func ReadRMQMessage(t testing.TB, queueName string, timeout time.Duration) ([]byte, error) {
	handlerName:="read-rmq-message"
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("[%s] Timeout waiting for message on queue %s", handlerName, queueName)
		case <-ticker.C:
			// Channel.Get pulls a single message synchronously.
			// autoAck = true automatically acknowledges and removes it from the queue for the test.
			msg, ok, err := rmq.RmqClient.Channel.Get(queueName, true)
			if err != nil {
				continue
			}
			if ok {
				return msg.Body, nil
			}
		}
	}
}