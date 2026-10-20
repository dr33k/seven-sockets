package presence

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"seven.com/sockets/auth"
	"seven.com/sockets/config"
	"seven.com/sockets/db/kfkdb"
	"seven.com/sockets/db/pgdb"
	"seven.com/sockets/db/rdsdb"
)

func LogPresence(c *gin.Context) {
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
		err = kfkdb.PubEvent(rctx, config.KAFKA_PRESENCE_TOPIC, kfkdb.Event{
			Type: "HEARTBEAT",
			AcctID:    acctIdStr,
			Timestamp: int64(now),
		})
		if err != nil{
			log.Printf("[%s] %v", handlerName, err)
			c.JSON(http.StatusInternalServerError, gin.H{"msg": "Unable to log presence event"})
			return
		}
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
			ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
			defer cancel()

			query := `
			INSERT INTO last_seen(acct_id, timestamp, updated_at) VALUES($1, TO_TIMESTAMP($2), NOW())
			ON CONFLICT (acct_id)
			DO UPDATE SET timestamp = EXCLUDED.timestamp, updated_at = NOW()
			`
			_, err := pgdb.DB.Exec(ctx, query, event.AcctID, event.Timestamp)
			if err != nil {
				log.Printf("[%s] Failed to persiste heartbeat to DB: %v", handlerName, err)
				return err
			}

			log.Printf("[%s] Successfully persisted heartbeat for user %s", handlerName, event.AcctID)
		}
		default: {
			log.Printf("[%s] Unknown event type: %s",handlerName, event.Type)
		}
	}
	return nil
}
