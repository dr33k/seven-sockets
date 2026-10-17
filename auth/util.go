package auth

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)



func GetAuthCtx(c *gin.Context)(*AuthContext, error){
	//Verify Auth Context exists
	authCtxVal, exists := c.Get("auth")
	if !exists {
		msg:="Account context not found"
		log.Println(msg)
		c.JSON(http.StatusUnauthorized, gin.H{"msg": msg})
		return nil, fmt.Errorf(msg)
	}

	// Retrieve Auth Context
	authCtx, ok := authCtxVal.(*AuthContext)
	if !ok {
		msg := fmt.Sprintf("Invalid AuthContext struct format: %v", authCtx)
		log.Println(msg)
		c.JSON(http.StatusConflict, gin.H{"msg": "Account context not found"})
		return nil, fmt.Errorf(msg)
	}

	return authCtx ,nil
}

