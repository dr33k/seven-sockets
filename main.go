package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"seven.com/sockets/internal/messaging"
	"seven.com/sockets/internal/presence"
	"seven.com/sockets/pkg/auth"
	"seven.com/sockets/pkg/db/mongodb"
	"seven.com/sockets/pkg/db/pgdb"
	"seven.com/sockets/pkg/db/rdsdb"
	"seven.com/sockets/pkg/db/rmq"
	"seven.com/sockets/pkg/env"
)

func main() {
	//Init Config
	env.InitEnv()

	//Init Redis
	rdsCtx, rdsCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rdsCancel()
	if err := rdsdb.InitRedis(rdsCtx, env.RedisUrl); err != nil {
		log.Fatalln(err.Error())
	}

	//Init Postgres
	pgCtx, pgCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pgCancel()
	if err := pgdb.InitPostgres(pgCtx, env.PgUrl); err != nil {
		log.Fatalln(err.Error())
	}
	defer pgdb.ClosePostgres()

	//Run Goose Migrations
	if err := pgdb.RunMigrations(); err != nil {
		log.Fatalln(err.Error())
	}

	//Init MongoDB
	mongoCtx, mongoCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mongoCancel()
	err := mongodb.InitMongoDB(mongoCtx, env.MongoUrl, env.MongoDbName)
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Defer graceful shutdown for mongo
	defer func() {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mongodb.CloseMongoDB(disconnectCtx); err != nil {
			log.Fatalln(err.Error())
		}
	}()

	//Init RMQ
	if err := rmq.InitRMQ(env.RmqUrl); err != nil {
		log.Fatalln(err.Error())
	}
	defer rmq.CloseRMQ()

	//Rmq Exchanges
	if err := rmq.DeclareExchange(env.RmqPresenceXName); err != nil {
		log.Fatalln(err.Error())
	}
	if err := rmq.DeclareExchange(env.RmqMsgXName); err != nil {
		log.Fatalln(err.Error())
	}

	//Rmq Main and DL Queues
	presenceQ, err := rmq.DeclareQueuePair(env.RmqPresenceQueueName, env.RmqPresenceXName, env.RmqPresenceRoutingKey, env.RmqPresenceDlxRoutingKey)
	if err != nil {
		log.Fatalln(err.Error())
	}

	msgQ, err := rmq.DeclareQueuePair(env.RmqMsgQueueName, env.RmqMsgXName, env.RmqMsgRoutingKey, env.RmqMsgDlxRoutingKey)
	if err != nil {
		log.Fatalln(err.Error())
	}

	//Start Rmq Consumers
	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()

	go func() {
		if err := rmq.StartConsumer(bgCtx, presenceQ, presence.PersistPresence); err != nil {
			log.Println(err.Error())
		}
	}()
	go func() {
		if err := rmq.StartConsumer(bgCtx, msgQ, messaging.PersistMsg); err != nil {
			log.Println(err.Error())
		}
	}()

	//Init Gin Router
	router := gin.Default()

	//Protect API
	protected := router.Group("/api")
	protected.Use(auth.JwtMW())
	{
		protected.POST("/presence", presence.LogPresence)
		protected.GET("/presence/:acct_id", presence.GetLastSeen)

		protected.GET("/ws", messaging.OpenWS)
	}

	//Wrap in HTTP server
	srvr := http.Server{
		Addr:    fmt.Sprintf(":%s", env.ServerPort),
		Handler: router,
	}

	//Http Server goroutine
	go func() {
		log.Printf("Starting server on port %v", env.ServerPort)
		if err := srvr.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not start server: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	<-sigChan

	bgCancel()

	//Timeout context with for server shutdown
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer timeoutCancel()
	if err := srvr.Shutdown(timeoutCtx); err != nil {
		log.Printf("Server Force Shutdown: %v", err)
	}

	log.Println("Gracefully terminated server")
}
