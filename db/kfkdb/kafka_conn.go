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
	AcctID    string `json:"acct_id"`
	Payload   string `json:"payload"`
	Timestamp int64  `json:"timestamp"`
}

var KafkaWriter *kafka.Writer

func InitKafkaProd() {
	handlerName:= "init-kafka-prod"
	brokerAddr := config.KafkaBrokerAddr

	KafkaWriter = &kafka.Writer{
		Addr:  kafka.TCP(brokerAddr),
		Balancer: &kafka.Hash{},
		BatchSize: 100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}

	log.Printf("[%s] Kafka producer connected to broker at %s", handlerName, brokerAddr)
}

func CloseKafkaProd(){
	handlerName:= "close-kafka-prod"
	if KafkaWriter != nil{
		if err := KafkaWriter.Close(); err != nil{
			log.Printf("[%s] Failed to close Kafka Writer: %v", handlerName, err)
		}
	}
}

func StartKafkaConsumer(ctx context.Context, handlerName, topic, groupID string, handler func(Event) error){

	brokerAddr:= config.KafkaBrokerAddr

	reader:= kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddr},
		Topic: topic,
		GroupID: groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait: 1*time.Second,
	})

	defer func(){
		if err:= reader.Close(); err != nil{
			log.Printf("[%s] Failed to close kafka reader: %v", handlerName, err)
		}
	}()

	log.Printf("[%s] Kafka Consumer started for topic '%s' (Group: '%s')", handlerName, topic, groupID)

	//Polling loop
	for{
		//Fetch Message blocks until a message is available or ctx is cancelled
		msg, err:= reader.FetchMessage(ctx)
		if err != nil{
			log.Printf("[%s] Consumer error on topic %s: %v", handlerName, topic, err)
			if ctx.Err() != nil{
				return
			}
			continue
		}
		log.Printf("[%s] New event for %s topic, groupID: %s", handlerName, topic, groupID)

		//Unmarshal message into PresenceEvent
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil{
			log.Printf("[%s] Failed to unmarshal event %s: %v", handlerName, string(msg.Value), err)
			//Commit the message anyway to the queue is not blocked
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		//Execute Handler function with retry loop

		//DLT topics have nil handlers
		if handler == nil{
			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("[%s] Failed to commit DLT message offset: %v", handlerName, err)
			}
			continue
		}

		handleEventSuccess := func(){
			log.Printf("[%s] %s event execution success", handlerName, event.Type)

			//Commit the message offset after successful processing
			if err:= reader.CommitMessages(ctx, msg); err != nil{
				log.Printf("[%s] Failed to commit message offset: %v", handlerName, err)
			}
		}

		handleEventFailure:= func ()  {
			log.Printf("[%s] Handler execution for event %+v failed after %v attempts, publishing to DLT", handlerName, event, config.KafkaMaxRetries)

			dlTopic:= fmt.Sprintf("%s-dlt", topic)
			err := KafkaWriter.WriteMessages(ctx, kafka.Message{
				Topic: dlTopic,
				Value: msg.Value,
			})

			if err != nil {
				log.Printf("[%s] CRITICAL: Failed to write message to DLT %s: %v", handlerName, dlTopic, err)
			} else {
				// Successfully sent to DLT, commit the original message offset so main consumer moves on
				if err := reader.CommitMessages(ctx, msg); err != nil {
					log.Printf("[%s] Failed to commit original message offset after DLT routing: %v", handlerName, err)
				}
			}			
		}

		handleRetries := func(){
			var handlerErr error
			success := false

			//Execute Handler
			for attempts:=1;attempts <= config.KafkaMaxRetries;attempts++{
				log.Printf("Retry attempt %v" ,attempts)
				
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
				handleEventSuccess()
			} else{
				handleEventFailure()
			}
		}

		err = handler(event)
		if err != nil{
			go handleRetries()
		} else{
			handleEventSuccess()
		}

	}
}