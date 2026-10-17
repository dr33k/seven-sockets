package main

import (
	"log"

	"github.com/gin-gonic/gin"
	"seven.com/sockets/auth"
	"seven.com/sockets/config"
	"seven.com/sockets/db/rdsdb"
	"seven.com/sockets/db/kfkdb"
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

	//Init Gin Router
	router := gin.Default()

	//Protect API
	protected := router.Group("/api")
	protected.Use(auth.JwtMW())
	{

		protected.POST("/heartbeat", presence.PersistHb)

	}

	router.Run(":8080")
}
