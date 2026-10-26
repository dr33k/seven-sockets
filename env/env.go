package env

import (
	"fmt"
	"os"
	"strconv"
)

var ServerPort = os.Getenv("SOCKETS_SERVER_PORT")
var JwtSecretKey = os.Getenv("JWT_SECRET_KEY")

var RedisPassword = os.Getenv("REDIS_PASSWORD")
var RedisUrl = os.Getenv("REDIS_URL")
var RedisPresenceKey = os.Getenv("REDIS_PRESENCE_KEY")

var rmqMaxRetriesStr = os.Getenv("RMQ_MAX_RETRIES")
var RmqMaxRetries int

var RmqPresenceExName = os.Getenv("RMQ_PRESENCE_EXCHANGE_NAME")
var RmqPresenceQueueName = os.Getenv("RMQ_PRESENCE_QUEUE_NAME")
var RmqPresenceRoutingKey = os.Getenv("RMQ_PRESENCE_ROUTING_KEY")
var RmqPresenceDlxRoutingKey = os.Getenv("RMQ_PRESENCE_DLX_ROUTING_KEY")

var PgUrl string
var RmqUrl string

func InitEnv() {
	if ServerPort == "" {
		ServerPort = "8080"
	}
	if JwtSecretKey == "" {
		JwtSecretKey = "test_weak_jwt_signing_key00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
	}
	if RedisUrl == "" {
		RedisUrl = "localhost:6379"
	}
	if RedisPresenceKey == "" {
		RedisPresenceKey = "presence:active_users"
	}
	if RmqPresenceExName == "" {
		RmqPresenceExName = "presence_exchange"
	}

	if rmqMaxRetriesStr == "" {
		rmqMaxRetriesStr = "3"
	} 
	val, err := strconv.Atoi(rmqMaxRetriesStr)
	if err == nil {
		RmqMaxRetries = val
	}else{
		RmqMaxRetries = 3
	}

	if RmqPresenceQueueName == "" {
		RmqPresenceQueueName = "presence_queue"
	}
	rmqhost := os.Getenv("RMQ_HOST")
	if rmqhost == "" {
		rmqhost = "localhost"
	}
	rmquser := os.Getenv("RMQ_USER")
	if rmquser == "" {
		rmquser = "guest"
	}
	rmqpass := os.Getenv("RMQ_PASSWORD")
	if rmqpass == "" {
		rmqpass = "guest"
	}
	rmqport := os.Getenv("RMQ_PORT")
	if rmqport == "" {
		rmqport = "5672"
	}
	RmqUrl = fmt.Sprintf("amqp://%s:%s@%s:%s", rmquser, rmqpass, rmqhost, rmqport)

	if RmqPresenceRoutingKey == "" {
		RmqPresenceRoutingKey = "account.presence.heartbeat"
	}
	if RmqPresenceDlxRoutingKey == "" {
		RmqPresenceDlxRoutingKey = "account.presence.dead"
	}
	pghost := os.Getenv("RMQ_HOST")
	if pghost == "" {
		pghost = "localhost"
	}
	pgdbname := os.Getenv("RMQ_DB_NAME")
	if pgdbname == "" {
		pgdbname = "sockets_db"
	}
	pguser := os.Getenv("RMQ_USER")
	if pguser == "" {
		pguser = "postgres"
	}
	pgpass := os.Getenv("RMQ_PASSWORD")
	if pgpass == "" {
		pgpass = "root"
	}
	pgport := os.Getenv("RMQ_PORT")
	if pgport == "" {
		pgport = "5430"
	}
	PgUrl = fmt.Sprintf("postgres://%s:%s@%s:%s/%s", pguser, pgpass, pghost, pgport, pgdbname)
}
