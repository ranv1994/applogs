package db

import (
	"time"
)

// DummyRedis implements Redis interface but does nothing
type DummyRedis struct {
	// We can add an in-memory cache here if needed
	cache map[string][]byte
}

// NewDummyRedis creates a new dummy Redis instance
func NewDummyRedis() *Redis {
	return &Redis{
		Client: nil,
	}
}

// Set implements dummy Set operation
func (r *DummyRedis) Set(key string, value interface{}, expiration time.Duration) error {
	// Do nothing, just return success
	return nil
}

// Get implements dummy Get operation
func (r *DummyRedis) Get(key string, value interface{}) error {
	// Always return cache miss
	return ErrCacheMiss
}

// Del implements dummy Del operation
func (r *DummyRedis) Del(key string) error {
	// Do nothing, just return success
	return nil
}

// Close implements dummy Close operation
func (r *DummyRedis) Close() error {
	// Do nothing, just return success
	return nil
}
