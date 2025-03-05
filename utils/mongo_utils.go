package utils

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// MongoUtils handles MongoDB operations
type MongoUtils struct{}

func NewMongoUtils() *MongoUtils {
	return &MongoUtils{}
}

// BuildDateRangeQuery creates a MongoDB query for date range
func (m *MongoUtils) BuildDateRangeQuery(startTs, endTs int32) bson.M {
	return bson.M{
		"timestamp": bson.M{
			"$gte": startTs,
			"$lte": endTs,
		},
	}
}

// ExecuteCountQuery executes a count query with timeout
func (m *MongoUtils) ExecuteCountQuery(ctx context.Context, collection *mongo.Collection, query bson.M) (int64, error) {
	count, err := collection.CountDocuments(ctx, query)
	return count, err
}
