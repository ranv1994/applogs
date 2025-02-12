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
	ErrCacheMiss = errors.New("cache miss")
)

type cacheItem struct {
	data       []byte
	expiration time.Time
}

type memoryCache struct {
	cache map[string]cacheItem
	mutex sync.RWMutex
}

type Redis struct {
	Client *redis.Client
	memory *memoryCache
}

func NewRedis(addr string, password string, db int) (*Redis, error) {
	// First create a Redis instance with memory cache initialized
	r := &Redis{
		memory: &memoryCache{
			cache: make(map[string]cacheItem),
		},
	}

	// Start cleanup routine for memory cache
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			r.memory.cleanup()
		}
	}()

	// Try to connect to Redis if credentials are provided
	if addr != "" {
		client := redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		})

		// Test the connection
		_, err := client.Ping(context.Background()).Result()
		if err == nil {
			r.Client = client
		}
	}

	return r, nil
}

// Memory cache methods
func (m *memoryCache) Set(key string, value []byte, expiration time.Time) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.cache[key] = cacheItem{
		data:       value,
		expiration: expiration,
	}
}

func (m *memoryCache) Get(key string) ([]byte, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	item, exists := m.cache[key]
	if !exists {
		return nil, false
	}

	if time.Now().After(item.expiration) {
		delete(m.cache, key)
		return nil, false
	}

	return item.data, true
}

func (m *memoryCache) Delete(key string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.cache, key)
}

func (m *memoryCache) cleanup() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	now := time.Now()
	for key, item := range m.cache {
		if now.After(item.expiration) {
			delete(m.cache, key)
		}
	}
}

// Redis interface methods
func (r *Redis) Set(key string, value interface{}, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	if r.Client != nil {
		return r.Client.Set(context.Background(), key, data, expiration).Err()
	}

	r.memory.Set(key, data, time.Now().Add(expiration))
	return nil
}

func (r *Redis) Get(key string, value interface{}) error {
	var data []byte
	var err error

	if r.Client != nil {
		data, err = r.Client.Get(context.Background(), key).Bytes()
		if err == redis.Nil {
			return ErrCacheMiss
		} else if err != nil {
			return err
		}
	} else {
		var exists bool
		data, exists = r.memory.Get(key)
		if !exists {
			return ErrCacheMiss
		}
	}

	return json.Unmarshal(data, value)
}

func (r *Redis) Del(key string) error {
	if r.Client != nil {
		return r.Client.Del(context.Background(), key).Err()
	}

	r.memory.Delete(key)
	return nil
}

func (r *Redis) Close() error {
	if r.Client != nil {
		return r.Client.Close()
	}
	return nil
}

// New methods to add after the existing Close() method:

// GetTTL returns the remaining time-to-live of a key
func (r *Redis) GetTTL(key string) (time.Duration, error) {
	if r.Client != nil {
		return r.Client.TTL(context.Background(), key).Result()
	}

	// For memory cache
	r.memory.mutex.RLock()
	defer r.memory.mutex.RUnlock()

	if item, exists := r.memory.cache[key]; exists {
		remaining := time.Until(item.expiration)
		if remaining > 0 {
			return remaining, nil
		}
		// Key has expired
		return -1, nil
	}
	// Key doesn't exist
	return -2, nil
}

// Exists checks if a key exists
func (r *Redis) Exists(key string) (bool, error) {
	if r.Client != nil {
		result, err := r.Client.Exists(context.Background(), key).Result()
		return result > 0, err
	}

	// For memory cache
	r.memory.mutex.RLock()
	defer r.memory.mutex.RUnlock()

	if item, exists := r.memory.cache[key]; exists {
		if time.Now().Before(item.expiration) {
			return true, nil
		}
		// Key has expired
		return false, nil
	}
	return false, nil
}

// SetNX sets a key only if it doesn't exist
func (r *Redis) SetNX(key string, value interface{}, expiration time.Duration) (bool, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return false, err
	}

	if r.Client != nil {
		return r.Client.SetNX(context.Background(), key, data, expiration).Result()
	}

	// For memory cache
	r.memory.mutex.Lock()
	defer r.memory.mutex.Unlock()

	if item, exists := r.memory.cache[key]; exists && time.Now().Before(item.expiration) {
		return false, nil
	}

	r.memory.cache[key] = cacheItem{
		data:       data,
		expiration: time.Now().Add(expiration),
	}
	return true, nil
}

// GetWithTTL gets a value and returns its remaining TTL
func (r *Redis) GetWithTTL(key string, value interface{}) (time.Duration, error) {
	if r.Client != nil {
		pipe := r.Client.Pipeline()
		getCmd := pipe.Get(context.Background(), key)
		ttlCmd := pipe.TTL(context.Background(), key)

		_, err := pipe.Exec(context.Background())
		if err == redis.Nil {
			return -1, ErrCacheMiss
		} else if err != nil {
			return -1, err
		}

		data, err := getCmd.Bytes()
		if err != nil {
			return -1, err
		}

		ttl, err := ttlCmd.Result()
		if err != nil {
			return -1, err
		}

		return ttl, json.Unmarshal(data, value)
	}

	// For memory cache
	r.memory.mutex.RLock()
	defer r.memory.mutex.RUnlock()

	if item, exists := r.memory.cache[key]; exists {
		remaining := time.Until(item.expiration)
		if remaining > 0 {
			return remaining, json.Unmarshal(item.data, value)
		}
		return -1, ErrCacheMiss
	}
	return -1, ErrCacheMiss
}

// SetMulti sets multiple key-value pairs with the same expiration
func (r *Redis) SetMulti(pairs map[string]interface{}, expiration time.Duration) error {
	if len(pairs) == 0 {
		return nil
	}

	if r.Client != nil {
		pipe := r.Client.Pipeline()
		for key, value := range pairs {
			data, err := json.Marshal(value)
			if err != nil {
				return err
			}
			pipe.Set(context.Background(), key, data, expiration)
		}
		_, err := pipe.Exec(context.Background())
		return err
	}

	// For memory cache
	r.memory.mutex.Lock()
	defer r.memory.mutex.Unlock()

	expirationTime := time.Now().Add(expiration)
	for key, value := range pairs {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		r.memory.cache[key] = cacheItem{
			data:       data,
			expiration: expirationTime,
		}
	}
	return nil
}

// GetMulti gets multiple keys at once
func (r *Redis) GetMulti(keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	if len(keys) == 0 {
		return result, nil
	}

	if r.Client != nil {
		pipe := r.Client.Pipeline()
		cmds := make(map[string]*redis.StringCmd)

		for _, key := range keys {
			cmds[key] = pipe.Get(context.Background(), key)
		}

		_, err := pipe.Exec(context.Background())
		if err != nil && err != redis.Nil {
			return nil, err
		}

		for key, cmd := range cmds {
			if data, err := cmd.Bytes(); err == nil {
				result[key] = data
			}
		}
		return result, nil
	}

	// For memory cache
	r.memory.mutex.RLock()
	defer r.memory.mutex.RUnlock()

	now := time.Now()
	for _, key := range keys {
		if item, exists := r.memory.cache[key]; exists && now.Before(item.expiration) {
			result[key] = item.data
		}
	}
	return result, nil
}
