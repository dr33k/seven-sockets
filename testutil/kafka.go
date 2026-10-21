package testutil

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/segmentio/kafka-go"
)

func CreateKafkaTopics(ctx context.Context, brokerAddr string, topics ...string) error {
	// Dial the broker to establish a management connection
	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		return fmt.Errorf("failed to dial kafka broker for topic creation: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get kafka controller: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port)))	
	if err != nil {
		return fmt.Errorf("failed to dial kafka controller: %w", err)
	}
	defer controllerConn.Close()

	topicConfigs := make([]kafka.TopicConfig, len(topics))
	for i, topic := range topics {
		topicConfigs[i] = kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return fmt.Errorf("failed to create kafka topics: %w", err)
	}

	log.Printf("[test-infra] Successfully created Kafka topics: %v", topics)
	return nil
}