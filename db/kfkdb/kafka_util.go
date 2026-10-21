package kfkdb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)


func PubEvent(ctx context.Context, topic string, event Event) error {
	handlerName := "kfk-pub-event-handler"

	if KafkaWriter == nil{
		return fmt.Errorf("[%s] Unititialized Kafka Writer", handlerName)
	}

	//Serialize into JSON
	payloadBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("[%s] Failed to marshal presence event %v", handlerName, event)
	}

	//Write to Topic
	err = KafkaWriter.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Value: payloadBytes,
	})

	if err != nil {
		return fmt.Errorf("[%s] failed to write message to kafka: %w", handlerName, err)
	}

	return nil
}
