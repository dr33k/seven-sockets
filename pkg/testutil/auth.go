package testutil

import (
	"fmt"
	"time"
	"uuid"

	"github.com/golang-jwt/jwt/v5"
	"seven.com/sockets/pkg/auth"
	"seven.com/sockets/pkg/env"
)

// GenerateTestToken creates a valid token on the fly for testing
func GenerateTestToken(accountID string, fname string) string {
	const handlerName = "generate-test-token"
	secretKey := []byte(env.JwtSecretKey)

	acctIDStr, err := uuid.Parse(accountID)
	if err != nil {
		panic(fmt.Errorf("[%s] Account ID Provided is not a UUID: %w", handlerName, err))
	}
	claims := jwt.MapClaims{
		"principal": auth.Acct{
			Id:    acctIDStr,
			Fname: fname,
		},
		"exp": time.Now().Add(24 * time.Hour).Unix(), // valid for 24 hours
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS512, claims)
	tokenString, _ := token.SignedString(secretKey)
	return tokenString
}
