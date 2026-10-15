package config

import "os"

var JwtSecretKey = os.Getenv("JWT_SECRET_KEY")

var RedisPassword = os.Getenv("REDIS_PASSWORD")
var RedisAddr = os.Getenv("REDIS_ADDR")
