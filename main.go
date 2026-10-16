package main

import (
	"log"

	"github.com/gin-gonic/gin"
	"seven.com/sockets/auth"
	"seven.com/sockets/config"
	"seven.com/sockets/db"
	"seven.com/sockets/presence"
)

func main() {
	//Init Config
	config.InitEnv()

	//Init Redis
	if err := db.InitRedis(); err != nil {
		log.Fatalf("Redis Initialization Error: %v", err)
	}

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
