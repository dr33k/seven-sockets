package kfkdb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)


func PubEvent(ctx context.Context, topic string, event Event) error {
	//Serialize into JSON
	payloadBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("Failed to marshal presence event %v", event)
	}

	//Write to Topic
	err = KafkaWriter.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Value: payloadBytes,
	})

	if err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	return nil
}
