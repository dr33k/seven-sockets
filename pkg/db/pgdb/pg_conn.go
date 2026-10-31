package pgdb

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

var DB *pgxpool.Pool

func InitPostgres(ctx context.Context, pgUrl string) error {
	handlerName := "init-postgres"

	poolConfig, err := pgxpool.ParseConfig(pgUrl)
	if err != nil {
		return fmt.Errorf("[%s] Failed to parse database env: %w", handlerName, err)
	}

	poolConfig.MaxConns = 25

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("[%s] Failed to create Connection Pool: %w", handlerName, err)
	}

	//Verify Connection
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("[%s] Failed to ping database: %w", handlerName, err)
	}

	DB = pool
	log.Printf("[%s] Postgres connection pool established successfully", handlerName)
	return nil
}

func ClosePostgres() {
	handlerName := "close-postgres"
	if DB != nil {
		DB.Close()
		log.Printf("[%s] Postgres Connection Pool closed successfully", handlerName)
	}
}
