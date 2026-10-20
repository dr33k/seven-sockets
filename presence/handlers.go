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
	"seven.com/sockets/db/kfkdb"
	"seven.com/sockets/db/rdsdb"
)

var LogPresence = func(c *gin.Context) {
	handlerName := "log-presence-handler"

	//Retrieve Auth Context
	authCtx, err := auth.GetAuthCtx(c)
	if err != nil {
		return
	}

	rctx := c.Request.Context()
	now := float64(time.Now().Unix()) // Unix timestamp
	acctIdStr := authCtx.Account.Id.String()

	//Publish Kafka Presence event if the account has no cache-recorded heartbeat or if it has been a minute since its last heartbeat
	acctScore := rdsdb.RedisClient.ZScore(rctx, config.REDIS_PRESENCE_KEY, acctIdStr)
	if acctScore == nil || now-acctScore.Val() > 60 {
		kfkdb.PubEvent(rctx, config.KAFKA_PRESENCE_TOPIC, kfkdb.Event{
			Type: "HEARTBEAT",
			AcctID:    acctIdStr,
			Timestamp: int64(now),
		})
	}

	//Update User's Heartbeat in Redis Sorted Set
	err = rdsdb.RedisClient.ZAdd(rctx, config.REDIS_PRESENCE_KEY, redis.Z{
		Score:  now,
		Member: acctIdStr,
	}).Err()

	if err != nil {
		msg := fmt.Sprintf("Failed to update account %v heartbeat", acctIdStr)
		log.Printf("[%s] %v: %v",handlerName, msg, err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"msg": msg})
		return
	}

	//Prune heartbearts more than 30 seconds old
	staleThreshold := now - 30
	err = rdsdb.RedisClient.ZRemRangeByScore(rctx, config.REDIS_PRESENCE_KEY, "-inf", fmt.Sprintf("%g", staleThreshold)).Err()

	if err != nil {
		log.Printf("[%s] Failed to prune old heartbeats: %v", handlerName, err.Error())
		c.JSON(http.StatusOK, gin.H{"msg": "Failed to prune old heartbeats"})
		return
	}

	log.Printf("[%s] Heartbeat from %v with id %v", handlerName, authCtx.Account.Fname, acctIdStr)
	c.JSON(http.StatusOK, nil)
}

func PersistPresence(event kfkdb.Event)error{
	handlerName:= "persist-presence-handler"
	log.Printf("[%s] %s presence event from user %s being persisted", handlerName, event.Type, event.AcctID)

	switch event.Type{
		case "HEARTBEAT":{
			log.Printf("[%s] Ready to persist heartbeat to db", handlerName)
		}
		default: {
			log.Printf("[%s] Unknown event type: %s",handlerName, event.Type)
		}
	}
	return nil
}
