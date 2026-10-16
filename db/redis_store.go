package db

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
	"seven.com/sockets/config"
)


var RedisClient *redis.Client

func InitRedis() error{
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
	ctx := context.Background()
	_, err := RedisClient.Ping(ctx).Result()
	if err != nil{
		return fmt.Errorf("Failed to connect to redis : %w", err)
	}
	log.Printf("Successfully connected to redis : %v", redisAddr)

	return nil
}