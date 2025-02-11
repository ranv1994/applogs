package db

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	redisClient *redis.Client
	redisOnce   sync.Once
)

var ErrCacheMiss = errors.New("cache miss")

type RedisInterface interface {
	Set(key string, value interface{}, expiration time.Duration) error
	Get(key string, value interface{}) error
	Del(key string) error
	Close() error
}

type Redis struct {
	Client *redis.Client
}

func NewRedis(addr string, password string, db int) (*Redis, error) {
	var err error
	redisOnce.Do(func() {
		redisClient = redis.NewClient(&redis.Options{
			Addr:     addr,     // Redis server address
			Password: password, // No password by default
			DB:       db,       // Default DB
			PoolSize: 100,      // Connection pool size
		})

		// Test the connection
		_, err = redisClient.Ping(context.Background()).Result()
	})

	if err != nil {
		return nil, err
	}

	return &Redis{Client: redisClient}, nil
}

func (r *Redis) Set(key string, value interface{}, expiration time.Duration) error {
	jsonData, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return r.Client.Set(context.Background(), key, jsonData, expiration).Err()
}

func (r *Redis) Get(key string, value interface{}) error {
	data, err := r.Client.Get(context.Background(), key).Result()
	if err == redis.Nil {
		return ErrCacheMiss
	} else if err != nil {
		return err
	}
	return json.Unmarshal([]byte(data), value)
}

func (r *Redis) Del(key string) error {
	return r.Client.Del(context.Background(), key).Err()
}

func (r *Redis) Close() error {
	if r.Client != nil {
		return r.Client.Close()
	}
	return nil
}
