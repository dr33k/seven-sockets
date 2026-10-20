package rdsdb

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
	"seven.com/sockets/config"
)


var RedisClient *redis.Client

func InitRedis(ctx context.Context) error{
	handlerName := "init-redis"

	redisAddr := config.RedisAddr
	redisPass := config.RedisPassword
	redisDB := 0

	RedisClient = redis.NewClient(&redis.Options{
		Addr: redisAddr,
		Password: redisPass,
		DB: redisDB,
		PoolSize: 100,
		MinIdleConns: 10,
	})

	//Test Connection
	_, err := RedisClient.Ping(ctx).Result()
	if err != nil{
		return fmt.Errorf("[%s] Failed to connect to redis : %v", handlerName, err)
	}

	log.Printf("[%s] Successfully connected to redis : %v", handlerName, redisAddr)
	return nil
}