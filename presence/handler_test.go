package presence

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
	"uuid"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"seven.com/sockets/auth"
	"seven.com/sockets/config"
	"seven.com/sockets/db/kfkdb"
	"seven.com/sockets/db/pgdb"
	"seven.com/sockets/db/rdsdb"
	"seven.com/sockets/testutil"
)

var testInfra *testutil.TestInfra

func TestMain(m *testing.M) {
	ctx := context.Background()

	// 1. Start Testcontainers before running any tests in this package
	var err error
	testInfra, err = testutil.SetupTestInfra(ctx)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup test infra: %v", err))
	}

	// 2. Override config/clients with test container endpoints
	config.PgUrl = testInfra.DBConnString
	if err:= pgdb.InitPostgres(ctx); err != nil {
		panic(fmt.Sprintf("Failed to setup Test Postgres Connection Pool: %v", err))
	}


	if err:= rdsdb.InitRedis(ctx,testInfra.RedisAddr); err != nil {
		panic(fmt.Sprintf("Failed to setup Test Redis Client: %v", err))
	}

	config.KafkaBrokerAddr = testInfra.KafkaBroker
	kfkdb.InitKafkaProd(testInfra.KafkaBroker);

	// Run tests
	code := m.Run()

	// 3. Cleanup containers after tests complete
	testInfra.Teardown(ctx)
	os.Exit(code)
}

// Helper function to set up a test router with the protected /api route
func setupTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	protected := r.Group("/api")
	protected.Use(auth.JwtMW())
	{
		protected.POST("/heartbeat", LogPresence)
	}
	return r
}

// Test 1: Unauthorized Request (Missing or Invalid Token)
func TestLogPresence_Unauthorized(t *testing.T) {
	router := setupTestRouter()

	// Create a request without an Authorization header / JWT token
	req, _ := http.NewRequest(http.MethodPost, "/api/heartbeat", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// Assert that the middleware blocks the request with 401 Unauthorized
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// Test 2: Successful First-Time Heartbeat (Passes Redis 60s check, triggers Kafka)
func TestLogPresence_SuccessNewHeartbeat(t *testing.T) {
	// Ensure Redis returns nil for ZScore (or a timestamp > 60s ago), and execute.

	acctID := uuid.New()
	ctx := context.Background()

	_ = rdsdb.RedisClient.Del(ctx, config.REDIS_PRESENCE_KEY).Err()

	// Generate a valid token for our test user
	testToken := testutil.GenerateTestToken(acctID, "John")

	router := setupTestRouter()
	req, _ := http.NewRequest(http.MethodPost, "/api/heartbeat", nil)
	
	// Inject a valid test bearer token header here
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", testToken))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Body.String())
}

// Test 3: Deduplicated Heartbeat (Redis score < 60s ago skips Kafka publish)
func TestLogPresence_Deduplicated(t *testing.T) {
	acctID := uuid.New()
	ctx := context.Background()

	_ = rdsdb.RedisClient.ZAdd(ctx, config.REDIS_PRESENCE_KEY, redis.Z{
		Score: float64(time.Now().Unix()-10), //10 seconds ago
		Member: "",
	})

	// Generate a valid token for our test user
	testToken := testutil.GenerateTestToken(acctID, "John")
		
	router := setupTestRouter()
	req, _ := http.NewRequest(http.MethodPost, "/api/heartbeat", nil)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", testToken))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Body.String())
}