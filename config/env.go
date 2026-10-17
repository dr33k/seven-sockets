package config

import "os"

var JwtSecretKey = os.Getenv("JWT_SECRET_KEY")
var RedisPassword = os.Getenv("REDIS_PASSWORD")
var RedisAddr = os.Getenv("REDIS_ADDR")
var KafkaBrokerAddr = os.Getenv("KAFKA_BROKER_ADDR")

func InitEnv() {
	if JwtSecretKey == "" {
		JwtSecretKey = "test_weak_jwt_signing_key00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
	}
	if RedisAddr == "" {
		RedisAddr = "localhost:6379"
	}
	if KafkaBrokerAddr == ""{
		KafkaBrokerAddr = "localhost:9092"
	}
}
