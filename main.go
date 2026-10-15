package main

import (
	"github.com/gin-gonic/gin"
	"seven.com/sockets/presence"
	"seven.com/sockets/auth"
)

func main(){
	router := gin.Default()

	//Protect API
	protected := router.Group("/api")
	protected.Use(auth.JwtMW())
	{

		protected.POST("/heartbeat", presence.PersistHb)

	}

	router.Run("localhost:8080")
}