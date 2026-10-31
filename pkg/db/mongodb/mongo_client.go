package mongodb

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)


type MongoDB struct{
	Client *mongo.Client
	Db *mongo.Database
}

func InitMongoDB(ctx context.Context, mongoUrl, dbName string)(*MongoDB, error){
	const componentName = "init-mongodb"

	//Configure client options
	clientOptions:= options.Client().
		ApplyURI(mongoUrl).
		SetMaxPoolSize(50).
		SetConnectTimeout(10*time.Second)

	//Connect to MongoDB
	client, err := mongo.Connect(clientOptions)
	if err != nil{
		return nil, fmt.Errorf("[%s] Failed to create mongo client: %w", componentName, err)
	}

	//Verify connection using a short lived timeout context
	pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
	defer cancelPing()

	if err := client.Ping(pingCtx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping mongodb server: %w", err)
	}

	log.Printf("[%s] Successfully connected and verified database: %s", componentName, dbName)

	return &MongoDB{
		Client: client,
		Db:     client.Database(dbName),
	}, nil
}

// Close gracefully disconnects the MongoDB client.
func (m *MongoDB) CloseMongoDB(ctx context.Context) error {
	const componentName = "client-mongodb"
	
	if err := m.Client.Disconnect(ctx); err != nil {
		return fmt.Errorf("failed to disconnect mongodb: %w", err)
	}
	
	log.Printf("[%s] Connection closed successfully", componentName)
	return nil
}