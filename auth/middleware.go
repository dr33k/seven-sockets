package auth

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"seven.com/sockets/config"
)


func JwtMW() gin.HandlerFunc{
	return func(ctx *gin.Context) {

		// Get Authorization Header
		authHeader :=  ctx.GetHeader("Authorization")
		if authHeader == ""{
			ctx.JSON(http.StatusUnauthorized, gin.H{"msg": "Unauthorized"})
			ctx.Abort()
			return 
		}

		//Extract Bearer Token
		tokens := strings.Split(authHeader, " ")
		if len(tokens) != 2 || tokens[0] != "Bearer"{
			ctx.JSON(http.StatusUnauthorized, gin.H{"msg": "Invalid Authorization Header"})
			ctx.Abort()
			return 
		}

		tokenString := tokens[1]

		//Parse and validate token
		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (any, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok{
				return nil, jwt.ErrSignatureInvalid
			}
			return config.JwtSecretKey, nil
		})

		if err != nil || !token.Valid{
			ctx.JSON(http.StatusUnauthorized, gin.H{"msg": "Invalid or Expired Token"})
			ctx.Abort()
			return 
		}

		//Map claims
		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			ctx.JSON(http.StatusUnauthorized, gin.H{"msg" : "Invalid Claims Format"})
			ctx.Abort()
			return 
		}

		ctx.Set("tenant", claims["tenant"])
		ctx.Set("acct", claims["principal"])
		ctx.Set("acct_id", claims["pincipal.id"])

		//Next Handler
		ctx.Next()
	}
}