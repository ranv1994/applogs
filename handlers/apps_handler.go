package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"app-logs/config"
	"app-logs/db"
)

type AppsHandler struct {
	cfg     *config.Config
	mongodb *db.MongoDB
	redisDB *db.Redis
}

type PostStr struct {
	Firebase     string `json:"firebase"`
	Ad_id        int32  `json:"ad_id"`
	Type         string `json:"type"`
	Ads_name     string `json:"ads_name"`
	Ads_position string `json:"ads_position"`
	From         string `json:"from"`
	Country      string `json:"country"`
	ComapnyId    int32  `json:"comapnyId,omitempty"` // Optional field
}

type HourlyImpression struct {
	Count        int64  `json:"count"`
	Range        string `json:"range"`
	IdBasedCount map[int32]int32
}

type HourlyResponse struct {
	Status     string             `json:"status"`
	Data       []HourlyImpression `json:"result,omitempty"`
	TotalCount int64              `json:"total_count,omitempty"`
	Date       *string            `json:"date,omitempty"`
}

type ErrorResponse struct {
	Status     string `json:"status"`
	Message    string `json:"message"`
	AppVersion string `json:"appVersion"`
}

type CustomResponseNew struct {
	Status        string   `json:"status"`
	Response      []bson.M `json:"response"`
	SearchingTime float64  `json:"searchingTime"`
	TotalPages    int      `json:"totalPages"`
	TotalItem     int      `json:"total_item"`
	AppVersion    string   `json:"appVersion"`
}

type CompanyAd struct {
	ID      json.Number `json:"id"`
	Company json.Number `json:"company"`
}

func NewAppsHandler(cfg *config.Config, mongodb *db.MongoDB, redisDB *db.Redis) *AppsHandler {
	return &AppsHandler{
		cfg:     cfg,
		mongodb: mongodb,
		redisDB: redisDB,
	}
}

func (h *AppsHandler) saveToCache(key string, data interface{}, expiration time.Duration) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return h.redisDB.Client.Set(context.Background(), key, jsonData, expiration).Err()
}

func (h *AppsHandler) getFromCache(key string, data interface{}) error {
	jsonData, err := h.redisDB.Client.Get(context.Background(), key).Result()
	if err == redis.Nil {
		return fmt.Errorf("key not found in cache")
	} else if err != nil {
		return err
	}
	return json.Unmarshal([]byte(jsonData), data)
}

func (h *AppsHandler) errorRes(c *gin.Context, message string) {
	response := ErrorResponse{
		Status:     "error",
		Message:    message,
		AppVersion: "3.0.1",
	}
	c.JSON(http.StatusOK, response)
}

func (h *AppsHandler) GetImpression(c *gin.Context) {
	startTime := time.Now()

	adID := c.Query("ad_id")
	adName := c.Query("ad_name")
	adPosition := c.Query("ad_position")
	firebase := c.Query("firebase")
	dates := c.Query("date")
	dbtype := c.Query("db_type")
	country := c.Query("country")

	filter := bson.M{}
	perPage, err := strconv.Atoi(c.DefaultQuery("perPage", "50"))
	if err != nil || perPage < 1 {
		h.errorRes(c, "Invalid perPage value")
		return
	}

	page, err := strconv.Atoi(c.DefaultQuery("page", "1"))
	if err != nil || page < 1 {
		h.errorRes(c, "Invalid page number")
		return
	}

	// Build filter based on query parameters
	if adID != "" {
		aId, err := strconv.Atoi(adID)
		if err != nil {
			h.errorRes(c, "Invalid aId")
			return
		}
		filter["ad_id"] = aId
	}

	if adName != "" {
		filter["ads_name"] = adName
	}

	if adPosition != "" {
		filter["ads_position"] = adPosition
	}

	if firebase != "" {
		filter["firebase"] = firebase
	}

	if dates != "" {
		dateRange := strings.Split(dates, "_")
		if len(dateRange) != 2 {
			h.errorRes(c, "Invalid date range format")
			return
		}

		firstDate, err := time.Parse("02-01-2006", dateRange[0])
		if err != nil {
			h.errorRes(c, "Invalid date format")
			return
		}

		secondDate, err := time.Parse("02-01-2006", dateRange[1])
		if err != nil {
			h.errorRes(c, "Invalid date format")
			return
		}
		secondDate = secondDate.AddDate(0, 0, 1)

		filter["timestamp"] = bson.M{"$gte": firstDate, "$lt": secondDate}
	}

	if country != "" {
		filter["country"] = strings.ToUpper(country)
	}

	skip := (page - 1) * perPage
	options := options.Find()
	options.SetLimit(int64(perPage))
	options.SetSkip(int64(skip))

	// Access the collection
	tableName := ""
	if dbtype == "click_info" {
		tableName = h.cfg.AppDBTableCli
	} else {
		tableName = h.cfg.AppDBTableImp
	}
	collection := h.mongodb.GetCollection(h.cfg.AppDBName, tableName)

	cursor, err := collection.Find(context.Background(), filter, options)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch data"})
		return
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err := cursor.All(context.Background(), &results); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decode data"})
		return
	}

	totalCount, err := collection.CountDocuments(context.Background(), filter)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get total count"})
		return
	}

	totalPages := int(math.Ceil(float64(totalCount) / float64(perPage)))
	searchingTime := time.Since(startTime).Seconds()

	response := CustomResponseNew{
		Status:        "ok",
		Response:      results,
		AppVersion:    "3.0.1",
		SearchingTime: searchingTime,
		TotalItem:     int(totalCount),
		TotalPages:    totalPages,
	}

	c.JSON(http.StatusOK, response)
}

func (h *AppsHandler) HourlyGetImpression(c *gin.Context) {
	adIDs := c.Query("ad_ids")
	adName := c.Query("ad_name")
	dbtype := c.Query("db_type")
	daysType := c.Query("days_type")
	country := c.Query("country")
	timeZone := c.Query("timeZone")
	dates := c.Query("date")

	istLocation, err := h.setTimeZone(timeZone)
	if err != nil {
		h.errorRes(c, err.Error())
		return
	}

	var tableName string
	if dbtype == "click_info" {
		tableName = h.cfg.AppDBTableCli
	} else {
		tableName = h.cfg.AppDBTableImp
	}
	collection := h.mongodb.GetCollection(h.cfg.AppDBName, tableName)
	filter := bson.M{}

	if adName != "" {
		filter["ads_name"] = adName
	}
	if country != "" {
		filter["country"] = country
	}

	if adIDs != "" {
		ids := strings.Split(adIDs, ",")
		adIDsInt := make([]int, len(ids))
		for i, id := range ids {
			adID, err := strconv.Atoi(id)
			if err != nil {
				h.errorRes(c, "Invalid ad ID")
				return
			}
			adIDsInt[i] = adID
		}
		filter["ad_id"] = bson.M{"$in": adIDsInt}
	}

	switch daysType {
	case "today":
		h.handleTodayImpression(c, collection, filter)
	case "weekly":
		h.handleWeeklyImpression(c, collection, filter, istLocation)
	case "month":
		h.handleMonthlyImpression(c, collection, filter)
	case "yesterday":
		h.handleYesterdayImpression(c, collection, filter, istLocation)
	case "custom":
		h.handleCustomDateImpression(c, collection, filter, dates)
	default:
		h.errorRes(c, "Query parameter required!")
	}
}

func (h *AppsHandler) GetTimezone(c *gin.Context) {
	collection := h.mongodb.GetCollection(h.cfg.AppDBName, h.cfg.AppDBTableTimezone)

	filter := bson.D{}
	projection := bson.D{{"_id", 0}}
	cursor, err := collection.Find(context.Background(), filter, options.Find().SetProjection(projection))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err := cursor.All(context.Background(), &results); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decode data"})
		return
	}
	c.JSON(http.StatusOK, results)
}

func (h *AppsHandler) SaveAppData(c *gin.Context) {
	var postData PostStr
	if err := c.ShouldBindJSON(&postData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON data"})
		return
	}

	tableName := ""
	if postData.From == "click" {
		tableName = h.cfg.AppDBTableCli
	} else {
		tableName = h.cfg.AppDBTableImp
	}
	collection := h.mongodb.GetCollection(h.cfg.AppDBName, tableName)

	atype, err := strconv.Atoi(postData.Type)
	if err != nil {
		h.errorRes(c, "Invalid Type")
		return
	}

	countryN := postData.Country
	if strings.Contains(postData.Country, "0") {
		countryN = h.countryNameReturn(postData.Country)
	}
	
	companyID := postData.ComapnyId
	if companyID == 0 {
		fetchedCompanyID, err := h.getcompanyId(postData.Ad_id)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to fetch company ID: %v", err)})
			return
		}
		companyID = fetchedCompanyID
	}

	document := bson.M{
		"firebase":  postData.Firebase,
		"timestamp": int32(time.Now().Unix()),
		"ad_id":     postData.Ad_id,
		"type":      atype,
		"ads_name":  postData.Ads_name,
		"country":   countryN,
		"company":   companyID,
	}

	_, err = collection.InsertOne(context.Background(), document)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save data"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"result": "saving task completed"})
}

func (h *AppsHandler) SaveAppDataTest(c *gin.Context) {
	var postData PostStr
	if err := c.ShouldBindJSON(&postData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON data"})
		return
	}

	tableName := ""
	if postData.From == "click" {
		tableName = h.cfg.AppDBTableCliTest
	} else {
		tableName = h.cfg.AppDBTableImpTest
	}
	collection := h.mongodb.GetCollection(h.cfg.AppDBName, tableName)

	atype, err := strconv.Atoi(postData.Type)
	if err != nil {
		h.errorRes(c, "Invalid Type")
		return
	}

	countryN := postData.Country
	if strings.Contains(postData.Country, "0") {
		countryN = h.countryNameReturn(postData.Country)
	}

	companyID := postData.ComapnyId
	if companyID == 0 {
		fetchedCompanyID, err := h.getcompanyId(postData.Ad_id)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to fetch company ID: %v", err)})
			return
		}
		companyID = fetchedCompanyID
	}

	document := bson.M{
		"firebase":  postData.Firebase,
		"timestamp": int32(time.Now().Unix()),
		"ad_id":     postData.Ad_id,
		"type":      atype,
		"ads_name":  postData.Ads_name,
		"country":   countryN,
		"company":   companyID,
	}

	_, err = collection.InsertOne(context.Background(), document)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save data"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"result": "saving test completed"})
}

// Helper methods
func (h *AppsHandler) getcompanyId(adId int32) (int32, error) {
	cacheKey := fmt.Sprintf("company_ad_%d", adId)
	var cachedCompany CompanyAd

	err := h.getFromCache(cacheKey, &cachedCompany)
	if err == nil {
		id, err := cachedCompany.Company.Int64()
		if err != nil {
			return 0, fmt.Errorf("invalid company ID format: %v", err)
		}
		return int32(id), nil
	}

	resp, err := http.Get("https://dev.cricket.entitysport.com/user/companies_ads")
	if err != nil {
		return 0, fmt.Errorf("error fetching JSON data: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("error reading response body: %v", err)
	}

	var companies []CompanyAd
	if err := json.Unmarshal(body, &companies); err != nil {
		log.Printf("Raw response: %s", string(body))
		return 0, fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	if err := h.saveToCache(cacheKey, companies, 10*time.Minute); err != nil {
		log.Printf("warning: failed to store data in cache: %v", err)
	}

	for _, company := range companies {
		id, err := company.ID.Int64()
		if err != nil {
			continue
		}
		if int32(id) == adId {
			companyID, err := company.Company.Int64()
			if err != nil {
				return 0, fmt.Errorf("invalid company ID format: %v", err)
			}
			return int32(companyID), nil
		}
	}

	return 0, errors.New("id not found")
}

func (h *AppsHandler) countryNameReturn(country string) string {
	countryMap := map[string]string{
		"+0430%u": "af",
		"+0330%u": "ir",
		"+06%u":   "bd",
		"+04%u":   "om",
		"+03%u":   "kw",
		"-03%u":   "br",
		"+10%u":   "ho",
		"+08%u":   "hk",
		"+0545%u": "np",
		"+0530%u": "in",
		"+05%u":   "pk",
		"+07%u":   "bkk",
		"-04%u":   "ca",
		"+0630%u": "ycdc",
	}

	if code, exists := countryMap[country]; exists {
		return code
	}
	return country
}

func (h *AppsHandler) setTimeZone(offset string) (*time.Location, error) {
	defaultOffset := "00:00"
	if offset == "" {
		offset = defaultOffset
	}

	parts := strings.Split(offset, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid timezone offset format")
	}

	hours, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid timezone offset format: %v", err)
	}

	minutes, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid timezone offset format: %v", err)
	}

	totalOffsetMinutes := hours*60 + minutes
	sign := 1
	if totalOffsetMinutes < 0 {
		sign = -1
		totalOffsetMinutes = -totalOffsetMinutes
	}

	offsetSeconds := sign * (totalOffsetMinutes * 60)
	return time.FixedZone(offset, offsetSeconds), nil
}

func (h *AppsHandler) getHoursInDayWithAMPM() []string {
	currentTime := time.Now()
	startOfDay := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), 0, 0, 0, 0, time.UTC)
	currentHour := currentTime.Hour()
	hoursInDay := []string{}

	for hour := 0; hour < currentHour; hour++ {
		startHour := startOfDay.Add(time.Duration(hour) * time.Hour)
		endHour := startHour.Add(time.Hour)
		hoursInDay = append(hoursInDay, fmt.Sprintf("%s_%s", startHour.Format("3:04pm"), endHour.Format("3:04pm")))
	}

	lastHour := startOfDay.Add(time.Duration(currentHour) * time.Hour)
	nextHour := lastHour.Add(time.Hour)
	hoursInDay = append(hoursInDay, fmt.Sprintf("%s_%s", lastHour.Format("3:04pm"), nextHour.Format("3:04pm")))

	return hoursInDay
}

func (h *AppsHandler) dateRangeSorting(rng string) (time.Time, error) {
	parts := strings.Split(rng, " - ")
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("invalid range format: %s", rng)
	}
	return time.Parse("2006-01-02", parts[0])
}

func (h *AppsHandler) extractTimeRange(hourRange string) (string, string) {
	parts := strings.Split(hourRange, "_")
	return parts[0], parts[1]
}

func (h *AppsHandler) handleTodayImpression(c *gin.Context, collection *mongo.Collection, filter bson.M) {
	hoursInDay := h.getHoursInDayWithAMPM()
	impressions := make([]HourlyImpression, len(hoursInDay))
	var totalCount int64

	currentTime := time.Now().UTC()
	startOfDay := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), 0, 0, 0, 0, time.UTC)
	endOfDay := startOfDay.Add(24 * time.Hour)

	filter["timestamp"] = bson.M{
		"$gte": startOfDay.Unix(),
		"$lt":  endOfDay.Unix(),
	}

	cursor, err := collection.Find(context.Background(), filter)
	if err != nil {
		h.errorRes(c, "Failed to fetch data")
		return
	}
	defer cursor.Close(context.Background())

	idBasedCount := make(map[string]map[int32]int32)
	for _, hourRange := range hoursInDay {
		idBasedCount[hourRange] = make(map[int32]int32)
	}

	for cursor.Next(context.Background()) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			continue
		}

		timestamp := int64(result["timestamp"].(int32))
		recordTime := time.Unix(timestamp, 0)
		hour := recordTime.Hour()

		if hour < len(hoursInDay) {
			impressions[hour].Count++
			impressions[hour].Range = hoursInDay[hour]

			if adID, ok := result["ad_id"].(int32); ok {
				if impressions[hour].IdBasedCount == nil {
					impressions[hour].IdBasedCount = make(map[int32]int32)
				}
				impressions[hour].IdBasedCount[adID]++
			}
		}
		totalCount++
	}

	currentDate := time.Now().Format("2006-01-02")
	response := HourlyResponse{
		Status:     "ok",
		Data:       impressions,
		TotalCount: totalCount,
		Date:       &currentDate,
	}

	c.JSON(http.StatusOK, response)
}

func (h *AppsHandler) handleWeeklyImpression(c *gin.Context, collection *mongo.Collection, filter bson.M, location *time.Location) {
	now := time.Now().In(location)
	startOfWeek := now.AddDate(0, 0, -int(now.Weekday()))
	startOfWeek = time.Date(startOfWeek.Year(), startOfWeek.Month(), startOfWeek.Day(), 0, 0, 0, 0, location)
	endOfWeek := startOfWeek.AddDate(0, 0, 7)

	filter["timestamp"] = bson.M{
		"$gte": startOfWeek.Unix(),
		"$lt":  endOfWeek.Unix(),
	}

	pipeline := []bson.M{
		{"$match": filter},
		{
			"$group": bson.M{
				"_id": bson.M{
					"$dateToString": bson.M{
						"format": "%Y-%m-%d",
						"date":   bson.M{"$toDate": bson.M{"$multiply": []interface{}{"$timestamp", 1000}}},
					},
				},
				"count": bson.M{"$sum": 1},
			},
		},
		{"$sort": bson.M{"_id": 1}},
	}

	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		h.errorRes(c, "Failed to fetch data")
		return
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err := cursor.All(context.Background(), &results); err != nil {
		h.errorRes(c, "Failed to process data")
		return
	}

	var impressions []HourlyImpression
	var totalCount int64

	for _, result := range results {
		count := result["count"].(int32)
		totalCount += int64(count)
		impressions = append(impressions, HourlyImpression{
			Count: int64(count),
			Range: result["_id"].(string),
		})
	}

	response := HourlyResponse{
		Status:     "ok",
		Data:       impressions,
		TotalCount: totalCount,
	}

	c.JSON(http.StatusOK, response)
}

func (h *AppsHandler) handleMonthlyImpression(c *gin.Context, collection *mongo.Collection, filter bson.M) {
	now := time.Now()
	startOfMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	endOfMonth := startOfMonth.AddDate(0, 1, 0)

	filter["timestamp"] = bson.M{
		"$gte": startOfMonth.Unix(),
		"$lt":  endOfMonth.Unix(),
	}

	pipeline := []bson.M{
		{"$match": filter},
		{
			"$group": bson.M{
				"_id": bson.M{
					"$dateToString": bson.M{
						"format": "%Y-%m-%d",
						"date":   bson.M{"$toDate": bson.M{"$multiply": []interface{}{"$timestamp", 1000}}},
					},
				},
				"count": bson.M{"$sum": 1},
			},
		},
		{"$sort": bson.M{"_id": 1}},
	}

	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		h.errorRes(c, "Failed to fetch data")
		return
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err := cursor.All(context.Background(), &results); err != nil {
		h.errorRes(c, "Failed to process data")
		return
	}

	var impressions []HourlyImpression
	var totalCount int64

	for _, result := range results {
		count := result["count"].(int32)
		totalCount += int64(count)
		impressions = append(impressions, HourlyImpression{
			Count: int64(count),
			Range: result["_id"].(string),
		})
	}

	response := HourlyResponse{
		Status:     "ok",
		Data:       impressions,
		TotalCount: totalCount,
	}

	c.JSON(http.StatusOK, response)
}

func (h *AppsHandler) handleYesterdayImpression(c *gin.Context, collection *mongo.Collection, filter bson.M, location *time.Location) {
	now := time.Now().In(location)
	startOfYesterday := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, location)
	endOfYesterday := startOfYesterday.Add(24 * time.Hour)

	filter["timestamp"] = bson.M{
		"$gte": startOfYesterday.Unix(),
		"$lt":  endOfYesterday.Unix(),
	}

	pipeline := []bson.M{
		{"$match": filter},
		{
			"$group": bson.M{
				"_id": bson.M{
					"hour": bson.M{
						"$hour": bson.M{
							"$toDate": bson.M{"$multiply": []interface{}{"$timestamp", 1000}},
						},
					},
				},
				"count": bson.M{"$sum": 1},
			},
		},
		{"$sort": bson.M{"_id.hour": 1}},
	}

	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		h.errorRes(c, "Failed to fetch data")
		return
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err := cursor.All(context.Background(), &results); err != nil {
		h.errorRes(c, "Failed to process data")
		return
	}

	impressions := make([]HourlyImpression, 24)
	var totalCount int64

	for i := 0; i < 24; i++ {
		startHour := startOfYesterday.Add(time.Duration(i) * time.Hour)
		endHour := startHour.Add(time.Hour)
		impressions[i].Range = fmt.Sprintf("%s_%s", startHour.Format("3:04pm"), endHour.Format("3:04pm"))
	}

	for _, result := range results {
		hour := result["_id"].(bson.M)["hour"].(int32)
		count := result["count"].(int32)
		if hour >= 0 && hour < 24 {
			impressions[hour].Count = int64(count)
			totalCount += int64(count)
		}
	}

	yesterdayDate := startOfYesterday.Format("2006-01-02")
	response := HourlyResponse{
		Status:     "ok",
		Data:       impressions,
		TotalCount: totalCount,
		Date:       &yesterdayDate,
	}

	c.JSON(http.StatusOK, response)
}

func (h *AppsHandler) handleCustomDateImpression(c *gin.Context, collection *mongo.Collection, filter bson.M, dates string) {
	dateRange := strings.Split(dates, "_")
	if len(dateRange) != 2 {
		h.errorRes(c, "Invalid date range format")
		return
	}

	startDate, err := time.Parse("02-01-2006", dateRange[0])
	if err != nil {
		h.errorRes(c, "Invalid start date format")
		return
	}

	endDate, err := time.Parse("02-01-2006", dateRange[1])
	if err != nil {
		h.errorRes(c, "Invalid end date format")
		return
	}
	endDate = endDate.AddDate(0, 0, 1)

	filter["timestamp"] = bson.M{
		"$gte": startDate.Unix(),
		"$lt":  endDate.Unix(),
	}

	pipeline := []bson.M{
		{"$match": filter},
		{
			"$group": bson.M{
				"_id": bson.M{
					"$dateToString": bson.M{
						"format": "%Y-%m-%d",
						"date":   bson.M{"$toDate": bson.M{"$multiply": []interface{}{"$timestamp", 1000}}},
					},
				},
				"count": bson.M{"$sum": 1},
			},
		},
		{"$sort": bson.M{"_id": 1}},
	}

	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		h.errorRes(c, "Failed to fetch data")
		return
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err := cursor.All(context.Background(), &results); err != nil {
		h.errorRes(c, "Failed to process data")
		return
	}

	var impressions []HourlyImpression
	var totalCount int64

	for _, result := range results {
		count := result["count"].(int32)
		totalCount += int64(count)
		impressions = append(impressions, HourlyImpression{
			Count: int64(count),
			Range: result["_id"].(string),
		})
	}

	response := HourlyResponse{
		Status:     "ok",
		Data:       impressions,
		TotalCount: totalCount,
	}

	c.JSON(http.StatusOK, response)
}