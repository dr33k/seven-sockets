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
	"seven.com/sockets/internal/db/pgdb"
	"seven.com/sockets/internal/db/rdsdb"
	"seven.com/sockets/internal/db/rmq"
	"seven.com/sockets/internal/presence"
	"seven.com/sockets/pkg/auth"
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

	//Init RMQ
	if err := rmq.InitRMQ(env.RmqUrl); err != nil {
		log.Fatalln(err.Error())
	}
	defer rmq.CloseRMQ()

	//Rmq Exchanges
	if err := rmq.DeclareExchange(env.RmqPresenceExName); err != nil {
		log.Fatalln(err.Error())
	}

	//Rmq Queues
	q, err := presence.DeclarePresenceQueues()
	if err != nil {
		log.Fatalln(err.Error())
	}

	//Start Rmq Consumers
	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()

	go func() {
		if err := rmq.StartConsumer(bgCtx, q, presence.PersistPresence); err != nil {
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
