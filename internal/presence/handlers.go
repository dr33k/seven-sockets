package presence

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"seven.com/sockets/pkg/db/pgdb"
	"seven.com/sockets/pkg/db/rdsdb"
	"seven.com/sockets/pkg/db/rmq"
	"seven.com/sockets/pkg/auth"
	"seven.com/sockets/pkg/env"
)

func LogPresence(c *gin.Context) {
	const handler = "log-presence-handler"

	if rdsdb.RedisClient == nil {
		log.Printf("[%s] Uninitialized RedisClient", handler)
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
		//QUEUE: Presence Persistence
		err = rmq.PubEvent(env.RmqPresenceXName, env.RmqPresenceRoutingKey, rmq.Event{
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
		log.Printf("[%s] %v: %v", handler, msg, err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"msg": msg})
		return
	}

	//Prune heartbearts more than 30 seconds old
	staleThreshold := now - 30
	err = rdsdb.RedisClient.ZRemRangeByScore(rctx, env.RedisPresenceKey, "-inf", fmt.Sprintf("%g", staleThreshold)).Err()

	if err != nil {
		log.Printf("[%s] Failed to prune old heartbeats: %v", handler, err.Error())
		c.JSON(http.StatusOK, gin.H{"msg": "Failed to prune old heartbeats"})
		return
	}

	log.Printf("[%s] Heartbeat from %v with id %v", handler, authCtx.Account.Fname, acctIdStr)
	c.Status(http.StatusOK)
}

func PersistPresence(event *rmq.Event) error {
	const handler = "persist-presence-handler"

	if pgdb.DB == nil {
		return fmt.Errorf("[%s] Uninitialized Postgres Pool", handler)
	}

	log.Printf("[%s] %s presence event from user %s being persisted", handler, event.Type, event.AcctID)

	switch event.Type {
	case "HEARTBEAT":
		{
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			query := `
			INSERT INTO acct_presence(acct_id, last_seen, updated_at) VALUES($1, TO_TIMESTAMP($2), NOW())
			ON CONFLICT (acct_id)
			DO UPDATE SET last_seen = EXCLUDED.last_seen, updated_at = NOW()
			`
			_, err := pgdb.DB.Exec(ctx, query, event.AcctID, event.Timestamp)
			if err != nil {
				log.Printf("[%s] Failed to persist heartbeat to DB: %v", handler, err)
				return err
			}

			log.Printf("[%s] Successfully persisted heartbeat for user %s", handler, event.AcctID)
		}
	default:
		{
			log.Printf("[%s] Unknown event type: %s", handler, event.Type)
		}
	}
	return nil
}

func GetLastSeen(c *gin.Context) {
	const handler = "get-heartbeat"
	ctx := c.Request.Context()
	acctID := c.Param("acct_id")

	if acctID == "" {
		log.Printf("[%s] Account ID not provided", handler)
		c.JSON(http.StatusBadRequest, gin.H{"msg": "Account ID not provided"})
		return
	}

	rdsLastSeen := rdsdb.RedisClient.ZScore(ctx, env.RedisPresenceKey, acctID)
	err := rdsLastSeen.Err()

	var lastSeenTime time.Time

	if err == nil { //Cache hit
		// Redis ZScore returns float64 (epoch seconds)
		score := rdsLastSeen.Val()
		lastSeenTime = time.Unix(int64(score), 0)

	} else if errors.Is(err, redis.Nil) { //Cache miss: Fetch from DB

		log.Printf("[%s] Cache miss for acct %s, falling back to database", handler, acctID)

		query := `select last_seen from acct_presence where acct_id=$1;`
		row := pgdb.DB.QueryRow(ctx, query, acctID)

		if scanErr := row.Scan(&lastSeenTime); scanErr != nil {
			log.Printf("[%s] Record for acct %s not found: %v in DB", handler, acctID, scanErr)
			c.JSON(http.StatusNotFound, gin.H{"msg": "Presence record not found for this account"})
			return
		}

		// --- BACKFILL REDIS (Cache-Aside Pattern) ---
		// Populate Redis so the next request is a cache hit
		score := float64(lastSeenTime.Unix())
		_ = rdsdb.RedisClient.ZAdd(ctx, env.RedisPresenceKey, redis.Z{
			Score:  score,
			Member: acctID,
		}).Err()

	} else {
		log.Printf("[%s] Error fetching last seen score from redis: %v", handler, err)
		c.JSON(http.StatusInternalServerError, gin.H{"msg": "An error occured on our end, please contact administrator"})
		return
	}

	log.Printf("[%s] Successsfully retrieved last seen records for acct %s", handler, acctID)
	c.JSON(http.StatusOK, gin.H{
		"acct_id":   acctID,
		"last_seen": lastSeenTime.Format(time.RFC3339),
	})
}
