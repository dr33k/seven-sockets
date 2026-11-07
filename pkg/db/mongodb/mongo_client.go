package mongodb

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var Client *mongo.Client
var DB *mongo.Database

func InitMongoDB(ctx context.Context, mongoUrl, dbName string) error {
	const componentName = "init-mongodb"

	//Configure client options
	clientOptions := options.Client().
		ApplyURI(mongoUrl).
		SetMaxPoolSize(50).
		SetConnectTimeout(10 * time.Second)

	//Connect to MongoDB
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return fmt.Errorf("[%s] Failed to create mongo client: %w", componentName, err)
	}

	//Verify connection
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("[%s] Failed to ping mongodb server: %w", componentName, err)
	}

	log.Printf("[%s] Successfully connected and verified database: %s", componentName, dbName)

	Client = client
	DB = client.Database(dbName)

	return nil
}

// Close gracefully disconnects the MongoDB client.
func CloseMongoDB(ctx context.Context) error {
	const componentName = "close-mongodb"

	if Client != nil {
		if err := Client.Disconnect(ctx); err != nil {
			return fmt.Errorf("[%s] Failed to disconnect mongodb: %w", componentName, err)
		}

		log.Printf("[%s] Connection closed successfully", componentName)
	} else{
		log.Printf("[%s] Mongodb Client has not been initialized yet", componentName)
	}
	return nil
}
