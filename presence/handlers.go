package presence

import (
	"fmt"
	"log"
	"net/http"
	"time"
	"uuid"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"seven.com/sockets/config"
	"seven.com/sockets/db"
)

var PersistHb = func(c *gin.Context) {

	//Retrieve acct_id from context
	acctIDVal, exists := c.Get("acct_id")
	if !exists {
		msg:="Account context not found"
		log.Println(msg)
		c.JSON(http.StatusUnauthorized, gin.H{"msg": msg})
		return
	}

	// Parse to UUID
	acctID, ok := acctIDVal.(uuid.UUID)
	if !ok {
		msg := "Invalid Account ID format"
		c.JSON(http.StatusInternalServerError, gin.H{"msg": msg})
		return
	}

	rctx := c.Request.Context()
	now := float64(time.Now().Unix()) // Unix timestamp

	//Update User's Heartbeat in Redis Sorted Set
	err := db.RedisClient.ZAdd(rctx, config.REDIS_PRESENCE_KEY, redis.Z{
		Score: now,
		Member: acctID,
	}).Err()

	if err != nil{
		msg := fmt.Sprintf("Failed to update account %v heartbeat", acctIDVal)
		c.JSON(http.StatusInternalServerError, gin.H{"msg": msg})
		return
	}

	//Prune heartbearts more than 30 seconds old
	staleThreshold := now-30
	err = db.RedisClient.ZRemRangeByScore(rctx, config.REDIS_PRESENCE_KEY, "-inf", string(rune(staleThreshold))).Err()

	if err != nil{
		msg := "Failed to prune old heartbeats"
		c.JSON(http.StatusOK, gin.H{"msg": msg})
		return
	}

	c.JSON(http.StatusOK, nil)
}
