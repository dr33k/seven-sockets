package presence

import (
	"context"
	// "encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	// "time"
	// "uuid"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"seven.com/sockets/auth"
	"seven.com/sockets/config"
	"seven.com/sockets/db/pgdb"
	"seven.com/sockets/db/rdsdb"
	"seven.com/sockets/db/rmq"
	"seven.com/sockets/testutil"
)

var testInfra *testutil.TestInfra

func TestMain(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())

	// 1. Start Testcontainers before running any tests in this package
	var err error
	testInfra, err = testutil.SetupTestInfra(ctx)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup test infra: %v", err))
	}

	if err:= rdsdb.InitRedis(ctx,testInfra.RedisUrl); err != nil {
		panic(fmt.Sprintf("Failed to setup Test Redis Client: %v", err))
	}

	// 2. Override config/clients with test container endpoints
	if err:= pgdb.InitPostgres(ctx, testInfra.PgUrl); err != nil {
		panic(fmt.Sprintf("Failed to setup Test Postgres Connection Pool: %v", err))
	}

	if err:=rmq.InitRMQ(testInfra.RmqUrl); err != nil{
		panic(fmt.Sprintf("Failed to setup Test RMQ: %v", err))
	}
	//Rmq Exchanges
	if err:=rmq.DeclareRmqExchange(config.RmqPresenceExName); err != nil{
		panic(fmt.Sprintf("Failed to Declare RMQ presence exchange: %v", err))
	}

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
		protected.POST("/heartbeat", LogPresence)
	}
	return r
}

// Test 1: Tests Unauthorized requests
func TestLogPresence_Unauthorized(t *testing.T) {
	router := setupTestRouter()

	// Create a request without an Authorization header / JWT token
	req, _ := http.NewRequest(http.MethodPost, "/api/heartbeat", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// Assert that the middleware blocks the request with 401 Unauthorized
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// // Test 2: Ensures presence logged more than 60 seconds ago or never-logged presences fire kafka publish events to persist them to DB
// func TestLogPresence_SuccessNewHeartbeat(t *testing.T) {
// 	acctID := uuid.New()
// 	ctx := context.Background()
// 	//Reader

// 	_ = rdsdb.RedisClient.Del(ctx, config.RedisUrl).Err()

// 	// Generate a valid token for our test user
// 	testToken := testutil.GenerateTestToken(acctID, "John")

// 	router := setupTestRouter()
// 	req, _ := http.NewRequest(http.MethodPost, "/api/heartbeat", nil)
	
// 	// Inject a valid test bearer token header here
// 	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", testToken))

// 	w := httptest.NewRecorder()
// 	router.ServeHTTP(w, req)

// 	// Assert successful request
// 	assert.Equal(t, http.StatusOK, w.Code)
// 	assert.Empty(t, w.Body.String())


// 	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
// 	defer cancel()

// 	//Read

// 	// Assert successful kafka publish
// 	if assert.NoError(t, err, "Expected a message to be published to Kafka, but got none"){
// 		assert.NotEmpty(t, msg.Value, "Kafka message body should not be empty")
// 	}

// 	var ev *kfkdb.Event
// 	err = json.Unmarshal(msg.Value, &ev)

// 	if assert.NoError(t, err, fmt.Sprintf("Expected no JSON unmarshalling error but got one: %v", err)){
// 		assert.Equal(t, ev.AcctID, acctID.String())
// 	}
// }

// // Test 3: Deduplicated Heartbeat (Tests that logs with Redis score < 60s ago skips Kafka publish)
// func TestLogPresence_Deduplicated(t *testing.T) {
// 	acctID := uuid.New()
// 	router := setupTestRouter()
// 	//Reader
	
// 	// Generate a valid token for our test user
// 	testToken := testutil.GenerateTestToken(acctID, "John")
	
// 	//First Request
// 	r1, _ := http.NewRequest(http.MethodPost, "/api/heartbeat", nil)
// 	r1.Header.Set("Authorization", fmt.Sprintf("Bearer %s", testToken))
// 	w1 := httptest.NewRecorder()
// 	router.ServeHTTP(w1, r1) //Creates first presence log 

// 	// Assert successful request
// 	assert.Equal(t, http.StatusOK, w1.Code)
// 	assert.Empty(t, w1.Body.String())

// 	//Immediate Second Request
// 	r2, _ := http.NewRequest(http.MethodPost, "/api/heartbeat", nil)
// 	r2.Header.Set("Authorization", fmt.Sprintf("Bearer %s", testToken))
// 	w2 := httptest.NewRecorder()
// 	router.ServeHTTP(w2, r2) //Creates second presence log by the same user almost immediately (less than 60s)
	
// 	// Assert successful request
// 	assert.Equal(t, http.StatusOK, w2.Code)
// 	assert.Empty(t, w2.Body.String())

	
// 	timeoutCtx1, timeoutCancel1 := context.WithTimeout(context.Background(), 10 * time.Second)
// 	defer timeoutCancel1()
// 	//Read

// 	//Assert that the first presence log was delivered by kafka
// 	if assert.NoError(t, err, "First presence log expected but recieved no event from kafka"){
// 		var ev *kfkdb.Event
// 		err = json.Unmarshal(msg.Value, &ev)

// 		if assert.NoError(t, err, fmt.Sprintf("Expected no JSON unmarshalling error but got one: %v", err)){
// 			assert.Equal(t, ev.AcctID, acctID.String(),"Expected first presence log's acctId and kafka event's acctId to be equal but was not")
// 		}
// 	}

// 	timeoutCtx2, timeoutCancel2 := context.WithTimeout(context.Background(), 10 * time.Second)
// 	defer timeoutCancel2()
// 	msg, err = kfkReader.ReadMessage(timeoutCtx2)

	
// 	//Assert that no other kafka event was found since the time between two logs for the same user was less than 60 seconds
// 	//Error being asserted is a timeout error
// 	assert.Error(t, err, "Expected a timeout error with no read messages but did not get one")
// }