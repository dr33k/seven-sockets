package config

import (
	"fmt"
	"os"
)
var ServerPort = os.Getenv("SOCKETS_SERVER_PORT")
var JwtSecretKey = os.Getenv("JWT_SECRET_KEY")
var RedisPassword = os.Getenv("REDIS_PASSWORD")
var RedisAddr = os.Getenv("REDIS_ADDR")
var KafkaBrokerAddr = os.Getenv("KAFKA_BROKER_ADDR")
var KafkaMaxRetries = 3
var KafkaBackoffMillis = 500

var PgUrl string

func InitEnv() {
	if ServerPort == ""{
		ServerPort = "8080"
	}
	if JwtSecretKey == "" {
		JwtSecretKey = "test_weak_jwt_signing_key00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
	}
	if RedisAddr == "" {
		RedisAddr = "localhost:6379"
	}
	if KafkaBrokerAddr == ""{
		KafkaBrokerAddr = "localhost:9092"
	}
	pghost := os.Getenv("POSTGRES_HOST")
	if pghost == ""{
		pghost = "localhost"
	}
	pgdbname := os.Getenv("POSTGRES_DB_NAME")
	if pgdbname == ""{
		pgdbname = "sockets_db"
	}
	pguser := os.Getenv("POSTGRES_USER")
	if pguser == ""{
		pguser = "postgres"
	}
	pgpass := os.Getenv("POSTGRES_PASSWORD")
	if pgpass == ""{
		pgpass = "root"
	}
	pgport := os.Getenv("POSTGRES_PORT")
	if pgport == ""{
		pgport = "5430"
	}
	PgUrl = fmt.Sprintf("postgres://%s:%s@%s:%s/%s", pguser, pgpass, pghost, pgport, pgdbname)
}
