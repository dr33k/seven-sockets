package kfkdb

import (
	"context"
	"encoding/json"
	"fmt"
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
		log.Printf("New event for %s topic, groupID: %s", topic, groupID)

		//Unmarshal message into PresenceEvent
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil{
			log.Printf("Failed to unmarshal event %s: %v", string(msg.Value), err)
			//Commit the message anyway to the queue is not blocked
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		//Execute Handler function with retry loop

		//DLT topics have nil handlers
		if handler == nil{
			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Failed to commit DLT message offset: %v", err)
			}
			continue
		}

		var handlerErr error
		success := false

		//Execute Handler
		for attempts:=1;attempts <= config.KafkaMaxRetries;attempts++{
			handlerErr = handler(event)
			if handlerErr == nil{
				success = true
				break
			}

			log.Printf("Retry attempt %v handler error : %v" ,attempts, handlerErr)

			if attempts < config.KafkaMaxRetries{
				backoff := time.Duration( 1<<(attempts-1) * time.Duration(config.KafkaBackoffMillis) * time.Millisecond)
				time.Sleep(backoff)
			}
		}

		// Post-Retry 
		if success{
			log.Printf("[%s] event execution success", event.Type)

			//Commit the message offset after successful processing
			if err:= reader.CommitMessages(ctx, msg); err != nil{
				log.Printf("Failed to commit message offset: %v", err)
			}
		} else{
			log.Printf("Handler execution for event %+v failed after %v attempts, publishing to DLT",event, config.KafkaMaxRetries)

			dlTopic:= fmt.Sprintf("%s-dlt", topic)
			err := KafkaWriter.WriteMessages(ctx, kafka.Message{
				Topic: dlTopic,
				Value: msg.Value,
			})

			if err != nil {
				log.Printf("CRITICAL: Failed to write message to DLT %s: %v", dlTopic, err)
			} else {
				// Successfully sent to DLT, commit the original message offset so main consumer moves on
				if err := reader.CommitMessages(ctx, msg); err != nil {
					log.Printf("Failed to commit original message offset after DLT routing: %v", err)
				}
			}
		}



	}
}