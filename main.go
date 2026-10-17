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
	"seven.com/sockets/auth"
	"seven.com/sockets/config"
	"seven.com/sockets/db/kfkdb"
	"seven.com/sockets/db/rdsdb"
	"seven.com/sockets/presence"
)

func main() {
	//Init Config
	config.InitEnv()

	//Init Redis
	if err := rdsdb.InitRedis(); err != nil {
		log.Fatalf("Redis Initialization Error: %v", err)
	}

	//Init Kafka
	kfkdb.InitKafkaProd()
	defer kfkdb.CloseKafkaProd()

	//Cancellable context with timeout for Kafka consumer and other bg processes
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	//Kafka Consumer goroutine
	go func(){
		kfkdb.StartKafkaConsumer(ctx, config.KAFKA_PRESENCE_TOPIC, config.KAFKA_PRESENCE_GROUP, presence.PersistPresence)
	}()

	//Init Gin Router
	router := gin.Default()

	//Wrap in HTTP server
	srvr := http.Server{
		Addr: fmt.Sprintf(":%s", config.ServerPort),
		Handler: router,
	}

	//Protect API
	protected := router.Group("/api")
	protected.Use(auth.JwtMW())
	{
		protected.POST("/heartbeat", presence.LogPresence)
	}

	//Http Server goroutine
	go func(){
		if err:= srvr.ListenAndServe(); err != nil{
			log.Fatalf("Could not start server: %v", err)
		}
		log.Printf("Server successfully started on port %v", config.ServerPort)
	}()


	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	<- sigChan

	if err:= srvr.Shutdown(ctx); err != nil{
		log.Printf("Server Force Shutdown: %v", err)
	}
	log.Println("Gracefully terminated server")
}
