package testutil

import (
	"time"
	"uuid"

	"github.com/golang-jwt/jwt/v5"
	"seven.com/sockets/auth"
	"seven.com/sockets/env"
)

// GenerateTestToken creates a valid token on the fly for testing
func GenerateTestToken(accountID uuid.UUID, fname string) string {
	secretKey := []byte(env.JwtSecretKey)

	claims := jwt.MapClaims{
		"principal": auth.Acct{
			Id:    accountID,
			Fname: fname,
		},
		"exp": time.Now().Add(24 * time.Hour).Unix(), // valid for 24 hours
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS512, claims)
	tokenString, _ := token.SignedString(secretKey)
	return tokenString
}
