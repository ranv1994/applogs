package handlers

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"

	"app-logs/config"
	"app-logs/db"
	"app-logs/utils"
)

type CappingHandler struct {
	cfg        *config.Config
	mongodb    *db.MongoDB
	cacheUtils *utils.CacheUtils
	respUtils  *utils.ResponseUtils
	capUtils   *utils.CappingUtils
	mysqlDB    *db.MySQL
}

func NewCappingHandler(cfg *config.Config, mongodb *db.MongoDB, redisDB *db.Redis, mysqlDB *db.MySQL) *CappingHandler {
	return &CappingHandler{
		cfg:        cfg,
		mongodb:    mongodb,
		cacheUtils: utils.NewCacheUtils(redisDB),
		respUtils:  utils.NewResponseUtils(),
		capUtils:   utils.NewCappingUtils(),
		mysqlDB:    mysqlDB,
	}
}

func (h *CappingHandler) UpdateCappingCount(c *gin.Context) {
	// Use caching to prevent frequent updates
	cacheKey := "capping_counts_update"
	var cachedResponse gin.H

	// Check if we have a cached response
	if err := h.cacheUtils.GetFromCache(cacheKey, &cachedResponse); err == nil {
		c.JSON(http.StatusOK, cachedResponse)
		return
	}

	// Get all active capping rules with a timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
		SELECT id, base, base_values, count, duration
		FROM app_ads_capping 
		WHERE status = 1
	`

	rows, err := h.mysqlDB.DB.QueryContext(ctx, query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to fetch capping rules",
		})
		return
	}
	defer rows.Close()

	var updatedRules []map[string]interface{}

	// Create a channel for concurrent processing
	resultChan := make(chan map[string]interface{}, 10)
	errorChan := make(chan error, 10)
	var wg sync.WaitGroup

	// Process rules concurrently with a worker pool
	semaphore := make(chan struct{}, 5) // Limit concurrent goroutines to 5

	for rows.Next() {
		var (
			id         int
			baseRule   string
			baseValues int
			count      int
			duration   string
		)

		if err := rows.Scan(&id, &baseRule, &baseValues, &count, &duration); err != nil {
			continue
		}

		wg.Add(1)
		go func(id int, baseRule string, baseValues int, count int, duration string) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire semaphore
			defer func() { <-semaphore }() // Release semaphore

			// Parse the base rule
			rule, err := h.capUtils.ParseBaseRule(baseRule)
			if err != nil {
				errorChan <- err
				return
			}

			// Build MongoDB query
			filter := h.buildCappingFilter(rule, duration)

			// Count documents with timeout context
			collection := h.mongodb.GetCollection(h.cfg.AppDBName, h.cfg.AppDBTableImp)
			newCount, err := collection.CountDocuments(ctx, filter)
			if err != nil {
				errorChan <- err
				return
			}

			// Update the count in MySQL asynchronously
			go func() {
				updateQuery := `
					UPDATE app_ads_capping 
					SET count = ?, updated = NOW()
					WHERE id = ?
				`
				_, err = h.mysqlDB.DB.ExecContext(ctx, updateQuery, newCount, id)
				if err != nil {
					log.Printf("Error updating count for rule %d: %v", id, err)
				}
			}()

			resultChan <- map[string]interface{}{
				"id":          id,
				"base_rule":   baseRule,
				"base_values": baseValues,
				"new_count":   newCount,
				"duration":    duration,
			}
		}(id, baseRule, baseValues, count, duration)
	}

	// Close channels when all goroutines are done
	go func() {
		wg.Wait()
		close(resultChan)
		close(errorChan)
	}()

	// Collect results
	for result := range resultChan {
		updatedRules = append(updatedRules, result)
	}

	response := gin.H{
		"status": "success",
		"data":   updatedRules,
	}

	// Cache the response for 5 minutes
	if err := h.cacheUtils.SaveToCache(cacheKey, response, 5*time.Minute); err != nil {
		log.Printf("Warning: Failed to cache capping counts: %v", err)
	}

	c.JSON(http.StatusOK, response)
}

// Helper methods
func (h *CappingHandler) buildCappingFilter(rule *utils.BaseRule, duration string) bson.M {
	filter := bson.M{}
	timeFilter := h.getTimestampFilter(duration)

	if rule.AdID != 0 {
		filter = bson.M{
			"timestamp": timeFilter,
			"ad_id":     rule.AdID,
		}
	} else if rule.AdIDs != "" {
		adIDs := strings.Split(rule.AdIDs, ",")
		var adIDInts []int32
		for _, idStr := range adIDs {
			if id, err := strconv.ParseInt(idStr, 10, 32); err == nil {
				adIDInts = append(adIDInts, int32(id))
			}
		}
		if len(adIDInts) > 0 {
			filter = bson.M{
				"timestamp": timeFilter,
				"ad_id":     bson.M{"$in": adIDInts},
			}
		}
	} else if rule.Company != 0 {
		filter = bson.M{
			"company":   rule.Company,
			"timestamp": timeFilter,
		}
	} else {
		filter = bson.M{
			"timestamp": timeFilter,
		}
	}

	if rule.Type != 0 {
		filter["type"] = rule.Type
	}

	return filter
}

func (h *CappingHandler) getTimestampFilter(duration string) bson.M {
	now := time.Now()

	switch duration {
	case "hourly":
		startTime := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, time.UTC)
		return bson.M{
			"$gte": startTime.Unix(),
			"$lt":  startTime.Add(time.Hour).Unix(),
		}
	case "daily":
		startTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		return bson.M{
			"$gte": startTime.Unix(),
			"$lt":  startTime.Add(24 * time.Hour).Unix(),
		}
	case "weekly":
		startTime := now.AddDate(0, 0, -int(now.Weekday()))
		startTime = time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, time.UTC)
		return bson.M{
			"$gte": startTime.Unix(),
			"$lt":  startTime.Add(7 * 24 * time.Hour).Unix(),
		}
	case "monthly":
		startTime := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
		return bson.M{
			"$gte": startTime.Unix(),
			"$lt":  startTime.AddDate(0, 1, 0).Unix(),
		}
	default: // Default to daily
		startTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		return bson.M{
			"$gte": startTime.Unix(),
			"$lt":  startTime.Add(24 * time.Hour).Unix(),
		}
	}
}
