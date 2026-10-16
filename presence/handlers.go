package presence

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"seven.com/sockets/auth"
	"seven.com/sockets/config"
	"seven.com/sockets/db"
)

var PersistHb = func(c *gin.Context) {

	//Verify Auth Context exists
	authCtxVal, exists := c.Get("auth")
	if !exists {
		msg:="Account context not found"
		log.Println(msg)
		c.JSON(http.StatusUnauthorized, gin.H{"msg": msg})
		return
	}

	// Retrieve Auth Context
	authCtx, ok := authCtxVal.(*auth.AuthContext)
	if !ok {
		log.Printf("Invalid AuthContext struct format: %v", authCtx)
		c.JSON(http.StatusConflict, gin.H{"msg": "Account context not found"})
		return
	}

	rctx := c.Request.Context()
	now := float64(time.Now().Unix()) // Unix timestamp
	acctIdStr := authCtx.Account.Id.String()

	//Update User's Heartbeat in Redis Sorted Set
	err := db.RedisClient.ZAdd(rctx, config.REDIS_PRESENCE_KEY, redis.Z{
		Score: now,
		Member: string(acctIdStr),
	}).Err()

	if err != nil{
		msg := fmt.Sprintf("Failed to update account %v heartbeat", acctIdStr)		
		log.Println(msg)
		c.JSON(http.StatusInternalServerError, gin.H{"msg": msg})
		return
	}

	//Prune heartbearts more than 30 seconds old
	staleThreshold := now-30
	err = db.RedisClient.ZRemRangeByScore(rctx, config.REDIS_PRESENCE_KEY, "-inf", fmt.Sprintf("%g", staleThreshold)).Err()

	if err != nil{
		log.Printf("Failed to prune old heartbeats: %v", err.Error())
		c.JSON(http.StatusOK, gin.H{"msg": "Failed to prune old heartbeats"})
		return
	}

	log.Printf("Heartbeat from %v with id %v", authCtx.Account.Fname, acctIdStr)
	c.JSON(http.StatusOK, nil)
}
