package kfkdb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)


func PubEvent(ctx context.Context, topic string, event Event) error {
	handlerName := "kfk-pub-event-handler"

	//Serialize into JSON
	payloadBytes, err := json.Marshal(event)
	if err != nil {
		msg:= fmt.Errorf("[%s] Failed to marshal presence event %v", handlerName, event)
		log.Println(msg)
		return msg
	}

	//Write to Topic
	err = KafkaWriter.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Value: payloadBytes,
	})

	if err != nil {
		msg:= fmt.Errorf("[%s] failed to write message to kafka: %w", handlerName, err)
		log.Println(msg)
		return msg
	}

	return nil
}
