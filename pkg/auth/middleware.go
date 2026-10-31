package auth

import (
	"log"
	"net/http"
	"strings"
	"time"
	"uuid"

	"github.com/Azure/go-autorest/autorest/date"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"seven.com/sockets/pkg/env"
)


type Acct struct{
	Id uuid.UUID `json:"id"`
	Fname string `json:"firstName"`
	Lname string `json:"lastName"`
	Email string `json:"email"`
	Phone string `json:"phoneNo"`
	Dob date.Date `json:"dob"`
	DateCreated time.Time `json:"dateCreated"`
	DateUpdated time.Time `json:"dateUpdated"`

}

type AuthContext struct{
	Account Acct `json:"principal"`
	Tenant string `json:"tenant"`
	jwt.RegisteredClaims
}

func JwtMW() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		handlerName := "jwt-mw-handler"

		// Get Authorization Header
		authHeader := ctx.GetHeader("Authorization")
		if authHeader == "" {
			log.Printf("[%s] No Authorization Header", handlerName)
			ctx.JSON(http.StatusUnauthorized, gin.H{"msg": "Unauthorized"})
			ctx.Abort()
			return
		}

		//Extract Bearer Token
		tokens := strings.Split(authHeader, " ")
		if len(tokens) != 2 || tokens[0] != "Bearer" {
			log.Printf("[%s] Unauthorized: Invalid Authorization Header", handlerName)
			ctx.JSON(http.StatusUnauthorized, gin.H{"msg": "Unauthorized"})
			ctx.Abort()
			return
		}

		tokenString := tokens[1]
		var authCtxClaims AuthContext

		//Parse and validate token
		token, err := jwt.ParseWithClaims(tokenString, &authCtxClaims, func(token *jwt.Token) (any, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, jwt.ErrSignatureInvalid
			}
			return []byte(env.JwtSecretKey), nil
		})

		if err != nil || !token.Valid {
			log.Printf("[%s] Invalid or Expired Token : %v", handlerName, err.Error())
			ctx.JSON(http.StatusUnauthorized, gin.H{"msg": "Unauthorized"})
			ctx.Abort()
			return
		}

		ctx.Set("auth", &authCtxClaims)

		//Next Handler
		ctx.Next()
	}
}
