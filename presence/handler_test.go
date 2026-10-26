package presence

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
	"uuid"

	"github.com/gin-gonic/gin"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"seven.com/sockets/auth"
	"seven.com/sockets/db/pgdb"
	"seven.com/sockets/db/rdsdb"
	"seven.com/sockets/db/rmq"
	"seven.com/sockets/env"
	"seven.com/sockets/testutil"
)

var testInfra *testutil.TestInfra

func TestMain(m *testing.M) {
	env.InitEnv()
	ctx, cancel := context.WithCancel(context.Background())

	// 1. Start Testcontainers before running any tests in this package
	var err error
	testInfra, err = testutil.SetupTestInfra(ctx)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup test infra: %v", err))
	}

	if err := rdsdb.InitRedis(ctx, testInfra.RedisUrl); err != nil {
		panic(fmt.Sprintf("Failed to setup Test Redis Client: %v", err))
	}

	// 2. Override env/clients with test container endpoints
	if err := pgdb.InitPostgres(ctx, testInfra.PgUrl); err != nil {
		panic(fmt.Sprintf("Failed to setup Test Postgres Connection Pool: %v", err))
	}

	if err := rmq.InitRMQ(testInfra.RmqUrl); err != nil {
		panic(fmt.Sprintf("Failed to setup Test RMQ: %v", err))
	}
	log.Printf("RMQ URL:%s", testInfra.RmqUrl)
	//Rmq Exchanges
	if err := rmq.DeclareExchange(env.RmqPresenceExName); err != nil {
		panic(err.Error())
	}

	//----------------------Rmq Presence DLQ and Main Queue---------------

	dlxName := env.RmqPresenceExName+"_dlx"
	dlqName:= env.RmqPresenceQueueName+"_dlx"

	//Dead Letter Queue
	_, dlerr := rmq.DeclareQueue(dlqName, dlxName, env.RmqPresenceDlxRoutingKey, nil)
	if dlerr != nil {
		panic(dlerr.Error())
	}

	qargs := amqp091.Table{
		"x-dead-letter-exchange":    dlxName,
		"x-dead-letter-routing-key": env.RmqPresenceDlxRoutingKey,
	}
	//Main Queue
	_, err = rmq.DeclareQueue(env.RmqPresenceQueueName, env.RmqPresenceExName, env.RmqPresenceRoutingKey, qargs)
	if err != nil {
		panic(err.Error())
	}

	//-----------------------------------------------------------------------

	
	// Run tests
	code := m.Run()

	// 3. Cleanup containers after tests complete
	testInfra.Teardown(ctx, cancel)
	os.Exit(code)
}

// Helper function to set up a test router with the protected /api route
func setupTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	protected := r.Group("/api")
	protected.Use(auth.JwtMW())
	{
		protected.POST("/presence", LogPresence)
		protected.GET("/presence/:acct_id", GetLastSeen)
	}
	return r
}

// Test 1: Tests Unauthorized requests
func TestLogPresence_Unauthorized(t *testing.T) {
	router := setupTestRouter()

	// Create a request without an Authorization header / JWT token
	req, _ := http.NewRequest(http.MethodPost, "/api/presence", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// Assert that the middleware blocks the request with 401 Unauthorized
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// Test 2: Ensures presence logged more than 60 seconds ago or never-logged presences fire kafka publish events to persist them to DB
func TestLogPresence_SuccessNewHeartbeat(t *testing.T) {
	acctID := uuid.New().String()
	ctx := context.Background()

	_ = rdsdb.RedisClient.Del(ctx, env.RedisUrl).Err()

	// Generate a valid token for our test user
	testToken := testutil.GenerateTestToken(acctID, "John")

	router := setupTestRouter()
	req, _ := http.NewRequest(http.MethodPost, "/api/presence", nil)

	// Inject a valid test bearer token header here
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", testToken))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assert successful request
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Body.String())

	bodyBytes, err := testutil.ReadRMQMessage(t, env.RmqPresenceQueueName, 5*time.Second)

	// Assert successful event publish
	if assert.NoError(t, err, "Expected a message to be published to Kafka, but got none") {
		assert.NotEmpty(t, bodyBytes, "MQ message body should not be empty")
	}

	var ev *rmq.Event
	err = json.Unmarshal(bodyBytes, &ev)

	if assert.NoError(t, err, fmt.Sprintf("Expected no JSON unmarshalling error but got one: %v", err)) {
		assert.Equal(t, ev.AcctID, acctID)
	}
}

// Test 3: Deduplicated Heartbeat (Tests that logs with Redis score < 60s ago skips Kafka publish)
func TestLogPresence_Deduplicated(t *testing.T) {
	acctID := uuid.New().String()
	router := setupTestRouter()

	// Generate a valid token for our test user
	testToken := testutil.GenerateTestToken(acctID, "John")

	//First Request
	r1, _ := http.NewRequest(http.MethodPost, "/api/presence", nil)
	r1.Header.Set("Authorization", fmt.Sprintf("Bearer %s", testToken))
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, r1) //Creates first presence log

	// Assert successful request
	assert.Equal(t, http.StatusOK, w1.Code)
	assert.Empty(t, w1.Body.String())

	//Immediate Second Request
	r2, _ := http.NewRequest(http.MethodPost, "/api/presence", nil)
	r2.Header.Set("Authorization", fmt.Sprintf("Bearer %s", testToken))
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, r2) //Creates second presence log by the same user almost immediately (less than 60s)

	// Assert successful request
	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Empty(t, w2.Body.String())

	bodyBytes, err := testutil.ReadRMQMessage(t, env.RmqPresenceQueueName, 5*time.Second)

	//Assert that the first presence log was delivered from the mq
	if assert.NoError(t, err, "First presence log expected but recieved no event from kafka"){
		var ev *rmq.Event
		err = json.Unmarshal(bodyBytes, &ev)

		if assert.NoError(t, err, fmt.Sprintf("Expected no JSON unmarshalling error but got one: %v", err)){
			assert.Equal(t, ev.AcctID, acctID,"Expected first presence log's acctId and MQ event's acctId to be equal but was not")
		}
	}

	_, err = testutil.ReadRMQMessage(t, env.RmqPresenceQueueName, 5*time.Second)

	//Assert that no other MQ event was found since the time between two logs for the same user was less than 60 seconds
	//Error being asserted is a timeout error
	assert.Error(t, err, "Expected a timeout error with no read messages but did not get one")
}

// Test 4: GetLastSeen - Cache Hit (Reads directly from Redis ZSet)
func TestGetLastSeen_CacheHit(t *testing.T) {
	acctID := uuid.New().String()
	ctx := context.Background()
	now := time.Now().Truncate(time.Second) // Truncate to second for epoch comparison
	score := float64(now.Unix())

	// 1. Seed Redis with the last seen score
	err := rdsdb.RedisClient.ZAdd(ctx, env.RedisPresenceKey, redis.Z{
		Score:  score,
		Member: acctID,
	}).Err()
	assert.NoError(t, err)
	
	// Ensure cleanup after test
	defer rdsdb.RedisClient.ZRem(ctx, env.RedisPresenceKey, acctID)

	// 2. Make GET request
	router := setupTestRouter()
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/api/presence/%s", acctID), nil)
	testToken := testutil.GenerateTestToken(acctID, "John")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", testToken))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// 3. Assertions
	assert.Equal(t, http.StatusOK, w.Code)
	
	// Optional: verify response body contains the correct timestamp string
	expectedTimeStr := now.Format(time.RFC3339)
	assert.Contains(t, w.Body.String(), acctID)
	assert.Contains(t, w.Body.String(), expectedTimeStr)
}

// Test 5: GetLastSeen - Cache Miss (Falls back to Postgres, returns 200, and backfills Redis)
func TestGetLastSeen_CacheMiss_DBFallback(t *testing.T) {
	acctID := uuid.New().String()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	// 1. Ensure Redis does NOT have the record (Cache Miss)
	_ = rdsdb.RedisClient.ZRem(ctx, env.RedisPresenceKey, acctID).Err()

	// 2. Insert mock record directly into PostgreSQL
	insertQuery := `insert into acct_presence (acct_id, last_seen) values ($1, $2) on conflict (acct_id) do update set last_seen = $2;`
	_, err := pgdb.DB.Exec(ctx, insertQuery, acctID, now)
	assert.NoError(t, err)

	defer func() {
		_, _ = pgdb.DB.Exec(ctx, `delete from acct_presence where acct_id = $1;`, acctID)
	}()

	// 3. Make GET request
	router := setupTestRouter()
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/api/presence/%s", acctID), nil)
	testToken := testutil.GenerateTestToken(acctID, "John")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", testToken))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// 4. Assertions for HTTP Response
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), acctID)

	// 5. Verify that Redis was backfilled (Cache-Aside pattern verification)
	score, err := rdsdb.RedisClient.ZScore(ctx, env.RedisPresenceKey, acctID).Result()
	assert.NoError(t, err, "Expected Redis to be backfilled after DB fallback")
	assert.Equal(t, float64(now.Unix()), score)

	// Cleanup Redis
	_ = rdsdb.RedisClient.ZRem(ctx, env.RedisPresenceKey, acctID).Err()
}

// Test 6: GetLastSeen - Test Not Found (Missing from both Redis and Postgres)
func TestGetLastSeen_NotFound(t *testing.T) {
	fakeAcctID := uuid.New().String()
	ctx := context.Background()

	// Ensure it's absent from both caches
	_ = rdsdb.RedisClient.ZRem(ctx, env.RedisPresenceKey, fakeAcctID).Err()
	_, _ = pgdb.DB.Exec(ctx, `delete from acct_presence where acct_id = $1;`, fakeAcctID)

	// Make GET request
	router := setupTestRouter()
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/api/presence/%s", fakeAcctID), nil)
	testToken := testutil.GenerateTestToken(fakeAcctID, "John")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", testToken))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assertions
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "Presence record not found")
}