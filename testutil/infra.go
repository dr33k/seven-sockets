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
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"seven.com/sockets/db/kfkdb"
	"seven.com/sockets/db/pgdb"
)

type TestInfra struct {
	PGContainer    *postgrescontainer.PostgresContainer
	RedisContainer *rediscontainer.RedisContainer
	KafkaContainer *redpanda.Container
	DBConnString   string
	RedisAddr      string
	KafkaBroker    string
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
		// .WithWaitStrategy(
		// 	wait.ForLog("database system is ready to accept connections").
		// 		WithOccurrence(2).
		// 		WithStartupTimeout(30*time.Second),
		// ),
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

	// 4. Redpanda (Kafka-compatible) Container
	kafkaC, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v23.3.3")
	if err != nil {
		return nil, fmt.Errorf("failed to start kafka/redpanda container: %w", err)
	}
	
	kafkaBroker, err := kafkaC.KafkaSeedBroker(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get kafka seed broker: %w", err)
	}
	
	err = CreateKafkaTopics(ctx, kafkaBroker, "heartbeat", "heartbeat-dlt")
	if err != nil {
		return nil, fmt.Errorf("failed to get Create Kafka topics: %w", err)
	}

	log.Println("[test-infra] Testcontainers (PG, Redis, Kafka) successfully initialized")

	return &TestInfra{
		PGContainer:    pgContainer,
		RedisContainer: redisC,
		KafkaContainer: kafkaC,
		DBConnString:   connStr,
		RedisAddr:      redisParsedEndpoint.Addr,
		KafkaBroker:	kafkaBroker,
	}, nil
}

func (t *TestInfra) Teardown(ctx context.Context) {
	// 1. Close application-level client pools first
	pgdb.ClosePostgres()
	kfkdb.CloseKafkaProd()
	
	if t.PGContainer != nil {
		_ = t.PGContainer.Terminate(ctx)
	}
	if t.RedisContainer != nil {
		_ = t.RedisContainer.Terminate(ctx)
	}
	if t.KafkaContainer != nil {
		_ = t.KafkaContainer.Terminate(ctx)
	}
	log.Println("[test-infra] Testcontainers terminated")
}