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


	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//Kafka presence Consumer goroutine
	go func(){
		kfkdb.StartKafkaConsumer(cancelCtx, "kfk-presence-csm", config.KAFKA_PRESENCE_TOPIC, config.KAFKA_PRESENCE_GROUP, presence.PersistPresence)
	}()
	//Kafka presence dlt Consumer goroutine
	go func(){
		kfkdb.StartKafkaConsumer(cancelCtx, "kfk-presence-dlt-csm", config.KAFKA_PRESENCE_DLT_TOPIC, config.KAFKA_PRESENCE_GROUP, nil)
	}()

	//Init Gin Router
	router := gin.Default()

	//Protect API
	protected := router.Group("/api")
	protected.Use(auth.JwtMW())
	{
		protected.POST("/heartbeat", presence.LogPresence)
	}

	//Wrap in HTTP server
	srvr := http.Server{
		Addr: fmt.Sprintf(":%s", config.ServerPort),
		Handler: router,
	}

	//Http Server goroutine
	go func(){
		log.Printf("Starting server on port %v", config.ServerPort)
		if err:= srvr.ListenAndServe(); err != nil && err != http.ErrServerClosed{
			log.Fatalf("Could not start server: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	<- sigChan

	cancel()

	//Cancellable context with timeout shutdown of bg processes
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer timeoutCancel()

	if err:= srvr.Shutdown(timeoutCtx); err != nil{
		log.Printf("Server Force Shutdown: %v", err)
	}
	log.Println("Gracefully terminated server")
}
