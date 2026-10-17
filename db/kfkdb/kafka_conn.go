package kfkdb

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"seven.com/sockets/config"
)

type Event struct {
	Type string `json:"event_type"`
	AcctID    string `json:"acctID"`
	Payload   string `json:"payload"`
	Timestamp int64  `json:"timestamp"`
}

var KafkaWriter *kafka.Writer

func InitKafkaProd() {
	brokerAddr := config.KafkaBrokerAddr

	KafkaWriter = &kafka.Writer{
		Addr:  kafka.TCP(brokerAddr),
		Balancer: &kafka.Hash{},
		BatchSize: 100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}

	log.Printf("Kafka producer connected to broker at %s", brokerAddr)
}

func CloseKafkaProd(){
	if KafkaWriter != nil{
		if err := KafkaWriter.Close(); err != nil{
			log.Printf("Failed to close Kafka Writer: %v", err.Error())
		}
	}
}

func StartKafkaConsumer(ctx context.Context, topic string, groupID string, handler func(Event) error){

	brokerAddr:= config.KafkaBrokerAddr

	reader:= kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddr},
		Topic: topic,
		GroupID: groupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	defer func(){
		if err:= reader.Close(); err != nil{
			log.Printf("Failed to close kafka reader: %v", err)
		}
	}()

	log.Printf("Kafka Consumer started for topic '%s' (Group: '%s')", topic, groupID)

	//Polling loop
	for{
		//Fetch Message blocks until a message is available or ctx is cancelled
		msg, err:= reader.FetchMessage(ctx)
		if err != nil{
			log.Printf("Consumer error on topic %s: %v", topic, err)
			if ctx.Err() != nil{
				return
			}
			continue
		}

		//Unmarshal message into PresenceEvent
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil{
			log.Printf("Failed to unmarshal event %s: %v", string(msg.Value), err)
			//Commit the message anyway to the queue is not blocked
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		//Execute Handler function
		if err := handler(event); err != nil{
			log.Printf("Error handling event: %v", err)
			//Handle retry logic
			continue
		}

		//Commit the message offset after successful processing
		if err:= reader.CommitMessages(ctx, msg); err != nil{
			log.Printf("Failed to commit message offset: %v", err)
		}
	}
}