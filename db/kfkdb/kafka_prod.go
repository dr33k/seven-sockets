package kfkdb

import (
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"seven.com/sockets/config"
)

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