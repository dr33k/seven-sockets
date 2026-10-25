package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"

	"github.com/pressly/goose/v3"
	"github.com/redis/go-redis/v9"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"seven.com/sockets/db/pgdb"
	"seven.com/sockets/db/rmq"
)

type TestInfra struct {
	PGContainer    *postgrescontainer.PostgresContainer
	RedisContainer *rediscontainer.RedisContainer
	RabbitContainer *rabbitmq.RabbitMQContainer
	PgUrl          string
	RedisUrl       string
	RmqUrl  string
}

const REL_PG_MIGRATIONS_PATH = "../db/pgdb/migrations"

func SetupTestInfra(ctx context.Context) (*TestInfra, error) {
	// 1. Spin up PostgreSQL Container
	pgContainer, err := postgrescontainer.Run(ctx,
		"postgres:16-alpine",
		postgrescontainer.WithDatabase("test_db"),
		postgrescontainer.WithUsername("postgres"),
		postgrescontainer.WithPassword("root"),
		postgrescontainer.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start postgres container: %w", err)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("failed to get postgres connection string: %w", err)
	}

	// 2. Run Embedded Migrations on the Test Container Database
	parsedURL, err := url.Parse(connStr)
	if err != nil {
		return nil, err
	}
	// Ensure pgx standard library driver can read it
	db, err := sql.Open("pgx", parsedURL.String())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	if err := goose.SetDialect("postgres"); err != nil {
		return nil, err
	}
	// Embedded migrations or relative path for tests
	if err := goose.Up(db, REL_PG_MIGRATIONS_PATH); err != nil {
		return nil, fmt.Errorf("failed to run migrations in test container: %w", err)
	}

	// 3. Spin up Redis Container
	redisC, err := rediscontainer.Run(ctx, "redis:7-alpine")
	if err != nil {
		return nil, fmt.Errorf("failed to start redis container: %w", err)
	}

	redisEndpoint, err := redisC.ConnectionString(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get redis endpoint: %w", err)
	}
	redisParsedEndpoint, err := redis.ParseURL(redisEndpoint) // redisEndpoint = "redis://localhost:PORT"
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis endpoint: %w", err)
	}

	// 4. Spin up RabbitMQ test container using the official management image
	rabbitContainer, err := rabbitmq.Run(ctx,
		"rabbitmq:3-management-alpine",
		rabbitmq.WithAdminUsername("guest"),
		rabbitmq.WithAdminPassword("guest"),
	)
	if err != nil {
		log.Fatalf("failed to start rabbitmq container: %v", err)
	}

	// Retrieve the dynamic AMQP connection URL assigned by testcontainers
	amqpURL, err := rabbitContainer.AmqpURL(ctx)
	if err != nil {
		log.Fatalf("failed to get rabbitmq amqp url: %v", err)
	}

	log.Println("[test-infra] Testcontainers (PG, Redis, RabbitMQ) successfully initialized")

	return &TestInfra{
		PGContainer:    pgContainer,
		RedisContainer: redisC,
		RabbitContainer: rabbitContainer,
		PgUrl:          connStr,
		RedisUrl:       redisParsedEndpoint.Addr,
		RmqUrl:  amqpURL,
	}, nil
}

func (t *TestInfra) Teardown(ctx context.Context, cancel context.CancelFunc) {
	// 1. Close application-level client pools first
	pgdb.ClosePostgres()
	rmq.CloseRMQ()

	if t.PGContainer != nil {
		_ = t.PGContainer.Terminate(ctx)
	}
	if t.RedisContainer != nil {
		_ = t.RedisContainer.Terminate(ctx)
	}
	if t != nil && t.RabbitContainer != nil {
		_=t.RabbitContainer.Terminate(ctx)
	}
	cancel()
	log.Println("[test-infra] Testcontainers terminated")
}
