package pgdb

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"seven.com/sockets/config"
)

var DB *pgxpool.Pool

func InitPostgres(ctx context.Context) error{
	handlerName := "init-postgres"
	dsn:= config.PgUrl

	poolConfig, err:= pgxpool.ParseConfig(dsn)
	if err != nil{
		return fmt.Errorf("[%s] Failed to parse database config: %w", handlerName, err)
	}

	poolConfig.MaxConns = 25

	pool, err:= pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("[%s] Failed to create Connection Pool: %w", handlerName, err)
	}

	//Verify Connection
	if err:= pool.Ping(ctx); err!=nil{
		return fmt.Errorf("[%s] Failed to ping database: %w", handlerName, err)
	}

	DB = pool
	log.Printf("[%s] Postgres connection pool established successfully", handlerName)
	return nil
}

func ClosePostgres(){
	handlerName := "close-postgres"
	if DB != nil{
		DB.Close()
		log.Printf("[%s] Postgres Connection Pool closed successfully", handlerName)
	}
}