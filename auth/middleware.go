package auth

import (
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"seven.com/sockets/config"
)

func JwtMW() gin.HandlerFunc {
	return func(ctx *gin.Context) {

		// Get Authorization Header
		authHeader := ctx.GetHeader("Authorization")
		if authHeader == "" {
			ctx.JSON(http.StatusUnauthorized, gin.H{"msg": "Unauthorized"})
			ctx.Abort()
			return
		}

		//Extract Bearer Token
		tokens := strings.Split(authHeader, " ")
		if len(tokens) != 2 || tokens[0] != "Bearer" {
			log.Println("UnauthorizedInvalid Authorization Header")
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
			return []byte(config.JwtSecretKey), nil
		})

		if err != nil || !token.Valid {
			log.Printf("Invalid or Expired Token : %v", err.Error())
			ctx.JSON(http.StatusUnauthorized, gin.H{"msg": "Unauthorized"})
			ctx.Abort()
			return
		}


		ctx.Set("auth", &authCtxClaims)

		//Next Handler
		ctx.Next()
	}
}
