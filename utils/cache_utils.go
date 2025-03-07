package utils

import (
	"app-logs/db"
	"time"
)

type CacheUtils struct {
	redisDB *db.Redis
}

func NewCacheUtils(redisDB *db.Redis) *CacheUtils {
	return &CacheUtils{
		redisDB: redisDB,
	}
}

func (c *CacheUtils) SaveToCache(key string, value interface{}, expiration time.Duration) error {
	if c.redisDB == nil {
		return nil // Silently ignore if no cache is available
	}
	return c.redisDB.Set(key, value, expiration)
}

func (c *CacheUtils) GetFromCache(key string, value interface{}) error {
	if c.redisDB == nil {
		return db.ErrCacheMiss
	}
	return c.redisDB.Get(key, value)
}
