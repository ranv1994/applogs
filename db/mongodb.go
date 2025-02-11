package db

import (
	"context"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	client     *mongo.Client
	clientOnce sync.Once
)

type MongoDB struct {
	Client *mongo.Client
}

func NewMongoDB(uri string) (*MongoDB, error) {
	var err error
	clientOnce.Do(func() {
		// Configure the client
		opts := options.Client().ApplyURI(uri)

		// Set up connection pool
		opts.SetMaxPoolSize(100) // Maximum number of connections in the pool
		opts.SetMinPoolSize(5)   // Minimum number of connections in the pool
		opts.SetMaxConnIdleTime(30 * time.Minute)

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Connect to MongoDB
		client, err = mongo.Connect(ctx, opts)
		if err != nil {
			return
		}

		// Ping the database
		if err = client.Ping(ctx, nil); err != nil {
			return
		}

		log.Println("Successfully connected to MongoDB")
	})

	if err != nil {
		return nil, err
	}

	return &MongoDB{Client: client}, nil
}

func (m *MongoDB) GetCollection(dbName, collName string) *mongo.Collection {
	return m.Client.Database(dbName).Collection(collName)
}

func (m *MongoDB) Close() {
	if m.Client != nil {
		if err := m.Client.Disconnect(context.Background()); err != nil {
			log.Printf("Error disconnecting from MongoDB: %v", err)
		}
	}
}
