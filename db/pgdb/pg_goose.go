package pgdb

import (
	"database/sql"
	"embed"
	"fmt"
	"log"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"seven.com/sockets/env"
)

//go:embed migrations/*.sql
var embedFS embed.FS

func RunMigrations() error {
	handlerName := "run-migrations"

	db, err := sql.Open("pgx", env.PgUrl)
	if err != nil {
		return fmt.Errorf("[%s] Could not open connection to DB : %w", handlerName, err)
	}
	defer db.Close()

	goose.SetBaseFS(embedFS)

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("[%s] Could not set DB connection dialect : %w", handlerName, err)
	}

	if err := goose.Up(db, "migrations"); err != nil {
		return fmt.Errorf("[%s] Could not execute migrations : %w", handlerName, err)
	}

	log.Printf("[%s] Database schema up to date", handlerName)
	return nil
}
