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
	"seven.com/sockets/db/pgdb"
	"seven.com/sockets/db/rdsdb"
	"seven.com/sockets/db/rmq"
	"seven.com/sockets/env"
)

func StartMQWorkers(ctx context.Context) error {
	//Rmq Queues
	q, err := rmq.DeclareQueue(env.RmqPresenceQueueName, env.RmqPresenceExName, env.RmqPresenceRoutingKey)
	if err != nil {
		return err
	}
	//Rmq Consumer worker
	go func() {
		if err := rmq.StartConsumer(ctx, q, PersistPresence); err != nil {
			log.Println(err.Error())
		}
	}()
	return nil
}

func LogPresence(c *gin.Context) {
	handlerName := "log-presence-handler"

	if rdsdb.RedisClient == nil {
		log.Printf("[%s] Uninitialized RedisClient", handlerName)
		c.JSON(http.StatusInternalServerError, gin.H{"msg": "There was an error on our end, please contact the administrator"})
		return
	}

	//Retrieve Auth Context
	authCtx, err := auth.GetAuthCtx(c)
	if err != nil {
		return
	}

	rctx := c.Request.Context()
	now := float64(time.Now().Unix()) // Unix timestamp
	acctIdStr := authCtx.Account.Id.String()

	//Publish Presence event if the account has no cache-recorded heartbeat or if it has been a minute since its last heartbeat
	acctScore := rdsdb.RedisClient.ZScore(rctx, env.RedisPresenceKey, acctIdStr)
	if acctScore == nil || now-acctScore.Val() > 60 {
		err = rmq.PubEvent(env.RmqPresenceExName, env.RmqPresenceRoutingKey, rmq.Event{
			Type:      "HEARTBEAT",
			AcctID:    acctIdStr,
			Timestamp: int64(now),
		})
		if err != nil {
			log.Println(err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"msg": "Unable to log presence event"})
			return
		}
	}

	//Update User's Heartbeat in Redis Sorted Set
	err = rdsdb.RedisClient.ZAdd(rctx, env.RedisPresenceKey, redis.Z{
		Score:  now,
		Member: acctIdStr,
	}).Err()

	if err != nil {
		msg := fmt.Sprintf("Failed to update account %v heartbeat", acctIdStr)
		log.Printf("[%s] %v: %v", handlerName, msg, err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"msg": msg})
		return
	}

	//Prune heartbearts more than 30 seconds old
	staleThreshold := now - 30
	err = rdsdb.RedisClient.ZRemRangeByScore(rctx, env.RedisPresenceKey, "-inf", fmt.Sprintf("%g", staleThreshold)).Err()

	if err != nil {
		log.Printf("[%s] Failed to prune old heartbeats: %v", handlerName, err.Error())
		c.JSON(http.StatusOK, gin.H{"msg": "Failed to prune old heartbeats"})
		return
	}

	log.Printf("[%s] Heartbeat from %v with id %v", handlerName, authCtx.Account.Fname, acctIdStr)
	c.Status(http.StatusOK)
}

func PersistPresence(event *rmq.Event) error {
	handlerName := "persist-presence-handler"

	if pgdb.DB == nil {
		return fmt.Errorf("[%s] Uninitialized Postgres Pool", handlerName)
	}

	log.Printf("[%s] %s presence event from user %s being persisted", handlerName, event.Type, event.AcctID)

	switch event.Type {
	case "HEARTBEAT":
		{
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			query := `
			INSERT INTO user_presence(acct_id, last_seen, updated_at) VALUES($1, TO_TIMESTAMP($2), NOW())
			ON CONFLICT (acct_id)
			DO UPDATE SET last_seen = EXCLUDED.last_seen, updated_at = NOW()
			`
			_, err := pgdb.DB.Exec(ctx, query, event.AcctID, event.Timestamp)
			if err != nil {
				log.Printf("[%s] Failed to persiste heartbeat to DB: %v", handlerName, err)
				return err
			}

			log.Printf("[%s] Successfully persisted heartbeat for user %s", handlerName, event.AcctID)
		}
	default:
		{
			log.Printf("[%s] Unknown event type: %s", handlerName, event.Type)
		}
	}
	return nil
}
