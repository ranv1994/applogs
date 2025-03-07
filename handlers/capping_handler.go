package handlers

import (
	"context"
	"fmt"
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

// Update the CappingRule struct with new field names
type CappingRule struct {
	ID              int       `db:"id"`
	Base            string    `db:"base"`
	BaseValuesImp   int       `db:"base_values_imp"`   // Renamed from base_values
	BaseValuesClick int       `db:"base_values_click"` // New field
	Status          int       `db:"status"`
	ImpCount        int       `db:"imp_count"` // Renamed from count
	ClickCount      int       `db:"click_count"`
	Duration        string    `db:"duration"`
	LimitBy         string    `db:"limit_by"`
	Created         time.Time `db:"created"`
	Updated         time.Time `db:"updated"`
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
	cacheKey := "capping_counts_update"
	var cachedResponse gin.H

	if err := h.cacheUtils.GetFromCache(cacheKey, &cachedResponse); err == nil {
		c.JSON(http.StatusOK, cachedResponse)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get current date in YYYY-MM-DD format
	currentDate := time.Now().Format("2006-01-02")

	// Updated query to filter by date based on limit_by
	query := `
		SELECT id, base, base_values_imp, base_values_click, status, 
			   imp_count, click_count, duration, limit_by, created, updated
		FROM app_ads_capping 
		WHERE status = 1 
		AND (
			(limit_by = 'daily' AND duration = ?)
			OR 
			(limit_by = 'overall' AND duration != 'not' AND 
			 ? BETWEEN SUBSTRING_INDEX(duration, '_', 1) AND SUBSTRING_INDEX(duration, '_', -1))
		)
	`

	rows, err := h.mysqlDB.DB.QueryContext(ctx, query, currentDate, currentDate)
	if err != nil {
		h.respUtils.ErrorResponse(c, "Failed to fetch capping rules")
		return
	}
	defer rows.Close()

	var updatedRules []map[string]interface{}
	resultChan := make(chan map[string]interface{}, 10)
	errorChan := make(chan error, 10)
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 5)

	for rows.Next() {
		var rule CappingRule
		if err := rows.Scan(
			&rule.ID,
			&rule.Base,
			&rule.BaseValuesImp,
			&rule.BaseValuesClick,
			&rule.Status,
			&rule.ImpCount,
			&rule.ClickCount,
			&rule.Duration,
			&rule.LimitBy,
			&rule.Created,
			&rule.Updated,
		); err != nil {
			continue
		}

		wg.Add(1)
		go func(rule CappingRule) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			baseRule, err := h.capUtils.ParseBaseRule(rule.Base)
			if err != nil {
				errorChan <- err
				return
			}

			newImpCount, newClickCount, err := h.getNewCounts(ctx, baseRule, rule)
			if err != nil {
				errorChan <- err
				return
			}

			totalImpCount := rule.ImpCount + newImpCount
			totalClickCount := rule.ClickCount + newClickCount

			go func() {
				updateQuery := `
					UPDATE app_ads_capping 
					SET imp_count = ?, click_count = ?, updated = NOW()
					WHERE id = ?
				`
				_, err = h.mysqlDB.DB.ExecContext(ctx, updateQuery, totalImpCount, totalClickCount, rule.ID)
				if err != nil {
					log.Printf("Error updating counts for rule %d: %v", rule.ID, err)
				}
			}()

			resultChan <- map[string]interface{}{
				"id":                rule.ID,
				"base_rule":         rule.Base,
				"base_values_imp":   rule.BaseValuesImp,
				"base_values_click": rule.BaseValuesClick,
				"new_imp_count":     newImpCount,
				"new_click_count":   newClickCount,
				"total_imp_count":   totalImpCount,
				"total_click_count": totalClickCount,
				"duration":          rule.Duration,
				"limit_by":          rule.LimitBy,
				"last_updated":      rule.Updated,
			}
		}(rule)
	}

	go func() {
		wg.Wait()
		close(resultChan)
		close(errorChan)
	}()

	for result := range resultChan {
		updatedRules = append(updatedRules, result)
	}

	response := gin.H{
		"status": "success",
		"data":   updatedRules,
	}

	if err := h.cacheUtils.SaveToCache(cacheKey, response, 5*time.Minute); err != nil {
		log.Printf("Warning: Failed to cache capping counts: %v", err)
	}

	c.JSON(http.StatusOK, response)
}

// Updated to return both impression and click counts
func (h *CappingHandler) getNewCounts(ctx context.Context, rule *utils.BaseRule, cappingRule CappingRule) (impCount int, clickCount int, err error) {
	timeFilter := h.getTimeFilter(cappingRule)
	if timeFilter == nil {
		return 0, 0, fmt.Errorf("invalid time filter for rule %d", cappingRule.ID)
	}

	// Build the base filter
	filter := h.buildCappingFilter(rule, timeFilter)

	// Get impression count
	impCollection := h.mongodb.GetCollection(h.cfg.AppDBName, h.cfg.AppDBTableImp)
	impCount64, err := impCollection.CountDocuments(ctx, filter)
	if err != nil {
		return 0, 0, err
	}

	// Get click count
	clickCollection := h.mongodb.GetCollection(h.cfg.AppDBName, h.cfg.AppDBTableCli)
	clickCount64, err := clickCollection.CountDocuments(ctx, filter)
	if err != nil {
		return 0, 0, err
	}

	return int(impCount64), int(clickCount64), nil
}

func (h *CappingHandler) getTimeFilter(cappingRule CappingRule) bson.M {
	switch cappingRule.LimitBy {
	case "overall":
		if cappingRule.Duration != "not" {
			dateRange := strings.Split(cappingRule.Duration, "_")
			if len(dateRange) == 2 {
				endDate, err := time.Parse("2006-01-02", dateRange[1])
				if err != nil {
					return nil
				}
				return bson.M{
					"$gte": cappingRule.Updated.Unix(),
					"$lte": endDate.Unix(),
				}
			}
		}

	case "daily":
		targetDate, err := time.Parse("2006-01-02", cappingRule.Duration)
		if err != nil {
			return nil
		}
		endOfDay := time.Date(targetDate.Year(), targetDate.Month(), targetDate.Day(), 23, 59, 59, 999999999, time.UTC)

		return bson.M{
			"$gte": cappingRule.Updated.Unix(),
			"$lte": endOfDay.Unix(),
		}
	}

	return nil
}

func (h *CappingHandler) buildCappingFilter(rule *utils.BaseRule, timeFilter bson.M) bson.M {
	filter := bson.M{"timestamp": timeFilter}

	if rule.AdID != 0 {
		filter["ad_id"] = rule.AdID
	} else if rule.AdIDs != "" {
		adIDs := strings.Split(rule.AdIDs, ",")
		var adIDInts []int32
		for _, idStr := range adIDs {
			if id, err := strconv.ParseInt(idStr, 10, 32); err == nil {
				adIDInts = append(adIDInts, int32(id))
			}
		}
		if len(adIDInts) > 0 {
			filter["ad_id"] = bson.M{"$in": adIDInts}
		}
	} else if rule.Company != 0 {
		filter["company"] = rule.Company
	}

	if rule.Type != 0 {
		filter["type"] = rule.Type
	}

	return filter
}

func (h *CappingHandler) UpdateCappingStatus(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get current date in YYYY-MM-DD format
	currentDate := time.Now().Format("2006-01-02")

	tx, err := h.mysqlDB.DB.BeginTx(ctx, nil)
	if err != nil {
		h.respUtils.ErrorResponse(c, "Failed to start transaction")
		return
	}
	defer tx.Rollback()

	// Updated select query with date filtering
	selectQuery := `
		SELECT id, base_values_imp, base_values_click, imp_count, click_count
		FROM app_ads_capping 
		WHERE status = 1 
		AND (
			(limit_by = 'daily' AND duration = ?)
			OR 
			(limit_by = 'overall' AND duration != 'not' AND 
			 ? BETWEEN SUBSTRING_INDEX(duration, '_', 1) AND SUBSTRING_INDEX(duration, '_', -1))
		)
		AND (
			(base_values_imp <= imp_count AND base_values_imp > 0)
			OR 
			(base_values_click <= click_count AND base_values_click > 0)
		)
	`

	rows, err := tx.QueryContext(ctx, selectQuery, currentDate, currentDate)
	if err != nil {
		h.respUtils.ErrorResponse(c, "Failed to fetch rules for update")
		return
	}

	type ruleToUpdate struct {
		ID              int
		BaseValuesImp   int
		BaseValuesClick int
		ImpCount        int
		ClickCount      int
	}

	var rulesToUpdate []ruleToUpdate
	for rows.Next() {
		var rule ruleToUpdate
		if err := rows.Scan(
			&rule.ID,
			&rule.BaseValuesImp,
			&rule.BaseValuesClick,
			&rule.ImpCount,
			&rule.ClickCount,
		); err != nil {
			rows.Close()
			h.respUtils.ErrorResponse(c, "Failed to scan rules")
			return
		}
		rulesToUpdate = append(rulesToUpdate, rule)
	}
	rows.Close()

	if len(rulesToUpdate) == 0 {
		// No rules to update
		c.JSON(http.StatusOK, gin.H{
			"status":       "success",
			"rows_updated": 0,
			"message":      "No rules needed status update",
			"timestamp":    time.Now().Format(time.RFC3339),
		})
		return
	}

	// Update the status
	updateQuery := `
		UPDATE app_ads_capping 
		SET status = 2 
		WHERE status = 1 
		AND (
			(base_values_imp <= imp_count AND base_values_imp > 0)
			OR 
			(base_values_click <= click_count AND base_values_click > 0)
		)
	`

	result, err := tx.ExecContext(ctx, updateQuery)
	if err != nil {
		h.respUtils.ErrorResponse(c, "Failed to update capping status")
		return
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		h.respUtils.ErrorResponse(c, "Failed to get affected rows count")
		return
	}

	// Insert logs for each updated rule
	insertLogQuery := `
		INSERT INTO app_ads_capping_logs 
		(capping_id, old_status, new_status, imp_count, click_count, base_values_imp, base_values_click) 
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`

	for _, rule := range rulesToUpdate {
		_, err = tx.ExecContext(ctx, insertLogQuery,
			rule.ID,              // capping_id
			1,                    // old_status
			2,                    // new_status
			rule.ImpCount,        // imp_count
			rule.ClickCount,      // click_count
			rule.BaseValuesImp,   // base_values_imp
			rule.BaseValuesClick, // base_values_click
		)
		if err != nil {
			log.Printf("Failed to insert log for rule %d: %v", rule.ID, err)
			h.respUtils.ErrorResponse(c, "Failed to insert capping log")
			return
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		h.respUtils.ErrorResponse(c, "Failed to commit transaction")
		return
	}

	response := gin.H{
		"status":        "success",
		"rows_updated":  rowsAffected,
		"updated_rules": rulesToUpdate,
		"timestamp":     time.Now().Format(time.RFC3339),
	}

	c.JSON(http.StatusOK, response)
}
