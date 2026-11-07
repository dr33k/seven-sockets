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

var RmqPresenceXName = os.Getenv("RMQ_PRESENCE_EXCHANGE_NAME")
var RmqPresenceQueueName = os.Getenv("RMQ_PRESENCE_QUEUE_NAME")
var RmqPresenceRoutingKey = os.Getenv("RMQ_PRESENCE_ROUTING_KEY")
var RmqPresenceDlxRoutingKey = os.Getenv("RMQ_PRESENCE_DLX_ROUTING_KEY")

var RmqMsgXName = os.Getenv("RMQ_MSG_EXCHANGE_NAME")
var RmqMsgQueueName = os.Getenv("RMQ_MSG_QUEUE_NAME")
var RmqMsgRoutingKey = os.Getenv("RMQ_MSG_ROUTING_KEY")
var RmqMsgDlxRoutingKey = os.Getenv("RMQ_MSG_DLX_ROUTING_KEY")

var MongoDbName = os.Getenv("MONGO_DB_NAME")

var PgUrl string
var RmqUrl string
var MongoUrl string

func InitEnv() {
	if ServerPort == "" {
		ServerPort = "8080"
	}
	if JwtSecretKey == "" {
		JwtSecretKey = "test_weak_jwt_signing_key00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
	}

	//-------------REDIS------------------------

	if RedisUrl == "" {
		RedisUrl = "localhost:6379"
	}
	if RedisPresenceKey == "" {
		RedisPresenceKey = "presence:active_users"
	}


	//-------------RABBIT-MQ------------------------

	if rmqMaxRetriesStr == "" {
		rmqMaxRetriesStr = "3"
	}
	val, err := strconv.Atoi(rmqMaxRetriesStr)
	if err == nil {
		RmqMaxRetries = val
	} else {
		RmqMaxRetries = 3
	}

	if RmqPresenceXName == "" {
		RmqPresenceXName = "presence_exchange"
	}

	if RmqPresenceQueueName == "" {
		RmqPresenceQueueName = "presence_queue"
	}

	if RmqPresenceRoutingKey == "" {
		RmqPresenceRoutingKey = "account.presence.heartbeat"
	}
	
	if RmqPresenceDlxRoutingKey == "" {
		RmqPresenceDlxRoutingKey = "account.presence.dead"
	}

	if RmqMsgXName == "" {
		RmqMsgXName = "msg_exchange"
	}

	if RmqMsgQueueName == "" {
		RmqMsgQueueName = "msg_queue"
	}

	if RmqMsgRoutingKey == "" {
		RmqMsgRoutingKey = "account.msg.new"
	}
	
	if RmqMsgDlxRoutingKey == "" {
		RmqMsgDlxRoutingKey = "account.msg.dead"
	}

	rmqhost := os.Getenv("RMQ_HOST")
	if rmqhost == "" {
		rmqhost = "localhost"
	}
	rmquser := os.Getenv("RABBITMQ_DEFAULT_USER")
	if rmquser == "" {
		rmquser = "guest"
	}
	rmqpass := os.Getenv("RABBITMQ_DEFAULT_PASS")
	if rmqpass == "" {
		rmqpass = "guest"
	}
	rmqport := os.Getenv("RMQ_PORT")
	if rmqport == "" {
		rmqport = "5672"
	}
	RmqUrl = fmt.Sprintf("amqp://%s:%s@%s:%s", rmquser, rmqpass, rmqhost, rmqport)


	//-------------POSTGRES------------------------

	pghost := os.Getenv("POSTGRES_HOST")
	if pghost == "" {
		pghost = "localhost"
	}
	pgdbname := os.Getenv("POSTGRES_DB")
	if pgdbname == "" {
		pgdbname = "sockets_db"
	}
	pguser := os.Getenv("POSTGRES_USER")
	if pguser == "" {
		pguser = "postgres"
	}
	pgpass := os.Getenv("POSTGRES_PASSWORD")
	if pgpass == "" {
		pgpass = "root"
	}
	pgport := os.Getenv("POSTGRES_PORT")
	if pgport == "" {
		pgport = "5430"
	}
	PgUrl = fmt.Sprintf("postgres://%s:%s@%s:%s/%s", pguser, pgpass, pghost, pgport, pgdbname)

	//--------------MONGO-DB---------------------
	
	mongohost := os.Getenv("MONGO_HOST")
	if mongohost == "" {
		mongohost = "localhost"
	}
	mongouser := os.Getenv("MONGO_INITDB_ROOT_USERNAME")
	if mongouser == "" {
		mongouser = "admin"
	}
	mongopass := os.Getenv("MONGO_INITDB_ROOT_PASSWORD")
	if mongopass == "" {
		mongopass = "root"
	}
	mongoport := os.Getenv("MONGO_PORT")
	if mongoport == "" {
		mongoport = "27017"
	}
	if MongoDbName == "" {
		MongoDbName = "sockets-db-mongodb"
	}
	MongoUrl = fmt.Sprintf("mongodb://%s:%s@%s:%s/?authSource=admin", mongouser, mongopass, mongohost, mongoport)
}
