package handlers

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"app-logs/config"
	"app-logs/db"
	"app-logs/utils"
)

type LogsHandler struct {
	cfg        *config.Config
	mongodb    *db.MongoDB
	cacheUtils *utils.CacheUtils
	respUtils  *utils.ResponseUtils
}

type LogData struct {
	IPAddress string `bson:"ip_address"`
	URL       string `bson:"url"`
	Token     string `bson:"token"`
	Date      string `bson:"date"`
	Timestamp int64  `bson:"timestamp"`
}

type CustomResponse struct {
	Status   string                 `json:"status"`
	Response map[string]interface{} `json:"response"`
	Date     string                 `json:"date"`
	APP      string                 `json:"api_version"`
}

func NewLogsHandler(cfg *config.Config, mongodb *db.MongoDB, redisDB *db.Redis) *LogsHandler {
	return &LogsHandler{
		cfg:        cfg,
		mongodb:    mongodb,
		cacheUtils: utils.NewCacheUtils(redisDB),
		respUtils:  utils.NewResponseUtils(),
	}
}

func (h *LogsHandler) APIHandler(c *gin.Context) {
	getFileName := c.Param("file")
	getFolderName := c.Param("folder")
	logTextFile := getFileName + ".txt"

	collection := h.mongodb.GetCollection(h.cfg.LogDBName, h.cfg.LogDBCompTable)
	uniqueEntries := make(map[string]struct{})

	fullPath := filepath.Join(h.cfg.DirPath, h.cfg.SubDirName, getFolderName, logTextFile)
	tempFullPath := filepath.Join(h.cfg.DirPath, h.cfg.SubDirName, getFolderName+h.cfg.TempLogFileName)
	time.Sleep(1 * time.Second)

	err := h.moveFileContents(fullPath, tempFullPath)
	if err != nil {
		log.Fatal(err)
	}

	err = h.processFileContent(tempFullPath, collection, &uniqueEntries)
	if err != nil {
		log.Fatal(err)
	}

	err = os.Truncate(fullPath, 0)
	if err != nil {
		log.Fatal(err)
	}

	err = os.Remove(tempFullPath)
	if err != nil {
		log.Fatal(err)
	}

	c.JSON(200, gin.H{"result": "Logs saving task completed"})
}

func (h *LogsHandler) APIFETCHANDLER(c *gin.Context) {
	keys := c.Request.URL.Query()
	perPage := c.Query("per_page")
	paged := c.Query("paged")

	if paged == "" {
		paged = "1"
	}

	var customResponse *CustomResponse
	var err error

	if token, ok := keys["token"]; ok {
		customResponse, err = h.dataGetFromDatabase(token[0], "token", perPage, paged)
	} else if ip, ok := keys["ip"]; ok {
		customResponse, err = h.dataGetFromDatabase(ip[0], "ip", perPage, paged)
	} else if _, ok := keys["error"]; ok {
		customResponse, err = h.dataGetFromDatabase("", "token", perPage, paged)
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid Parameter"})
		return
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, customResponse)
}

func (h *LogsHandler) APIFatchAllTokens(c *gin.Context) {
	keys := c.Request.URL.Query()
	var keySlice []string
	for key := range keys {
		keySlice = append(keySlice, key)
	}

	paged := keys.Get("paged")
	if paged == "" {
		paged = "1"
	}

	collection := h.mongodb.GetCollection(h.cfg.LogDBName, h.cfg.LogDBCompTable)
	perPageInt, _, err := h.perPageItem(collection, keys.Get("per_page"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	pagedInt, err := strconv.Atoi(paged)
	if err != nil || pagedInt <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid paged value"})
		return
	}

	pipeline := bson.A{
		bson.D{{"$group", bson.D{{"_id", "$token"}, {"count", bson.D{{"$sum", 1}}}}}},
		bson.D{{"$match", bson.D{{"count", bson.D{{"$gt", 1}}}}}},
		bson.D{{"$sort", bson.D{{"count", -1}}}},
		bson.D{{"$skip", (pagedInt - 1) * perPageInt}},
		bson.D{{"$limit", perPageInt}},
	}

	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err := cursor.All(context.Background(), &results); err != nil {
		log.Fatal(err)
	}

	var response []map[string]interface{}
	for _, result := range results {
		token := result["_id"].(string)
		count := result["count"].(int32)
		response = append(response, map[string]interface{}{"token": token, "total_ip_address": count})
	}

	currentDateTime := time.Now().Format("2006-01-02 : 15:04:05")
	totalItems, err := collection.CountDocuments(context.Background(), bson.D{})
	if err != nil {
		log.Fatal(err)
	}
	totalPages := int(math.Ceil(float64(totalItems) / float64(perPageInt)))

	finalResponse := CustomResponse{
		Status: "ok",
		Response: map[string]interface{}{
			"items":       response,
			"total_items": totalItems,
			"total_pages": totalPages,
		},
		Date: currentDateTime,
		APP:  "3.0.0",
	}

	c.JSON(http.StatusOK, finalResponse)
}

// Helper methods
func (h *LogsHandler) moveFileContents(src, dest string) error {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer destination.Close()

	_, err = io.Copy(destination, source)
	return err
}

func (h *LogsHandler) processFileContent(filePath string, collection *mongo.Collection, uniqueEntries *map[string]struct{}) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var insertedCount, skippedCount int

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, " | ")

		ipAddress := fields[0]
		url := fields[1]
		date := fields[2]
		timestampStr := fields[3]
		token := ""

		tokenParts := strings.Split(url, "token=")
		if len(tokenParts) > 1 {
			token = strings.Split(tokenParts[1], "&")[0]
		}

		entryKey := ipAddress + "_" + token

		if _, ok := (*uniqueEntries)[entryKey]; !ok {
			(*uniqueEntries)[entryKey] = struct{}{}

			filter := bson.D{
				{"ip_address", ipAddress},
				{"token", token},
			}

			var existingData LogData
			err := collection.FindOne(context.Background(), filter).Decode(&existingData)
			if err != nil {
				if err != mongo.ErrNoDocuments {
					return err
				}

				timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
				if err != nil {
					return err
				}

				logData := LogData{
					IPAddress: ipAddress,
					URL:       url,
					Token:     token,
					Date:      date,
					Timestamp: timestamp,
				}

				_, err = collection.InsertOne(context.Background(), logData)
				if err != nil {
					return err
				}
				insertedCount++
			} else {
				skippedCount++
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	fmt.Printf("Inserted %d unique items and skipped %d duplicate items\n", insertedCount, skippedCount)
	return nil
}

func (h *LogsHandler) dataGetFromDatabase(keyValue string, keyName string, perPage string, paged string) (*CustomResponse, error) {
	collection := h.mongodb.GetCollection(h.cfg.LogDBName, h.cfg.LogDBCompTable)

	perPageInt, _, err := h.perPageItem(collection, perPage)
	if err != nil {
		return nil, err
	}

	pagedInt, err := strconv.Atoi(paged)
	if err != nil || pagedInt <= 0 {
		return nil, err
	}

	var filter bson.D
	if keyName == "token" {
		filter = bson.D{{"token", keyValue}}
	} else if keyName == "ip" {
		filter = bson.D{{"ip_address", keyValue}}
	} else if keyName == "error" {
		filter = bson.D{{"token", keyValue}}
	}

	pipeline := bson.A{
		bson.D{{"$match", filter}},
		bson.D{{"$skip", (pagedInt - 1) * perPageInt}},
		bson.D{{"$limit", perPageInt}},
	}

	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	var documents []bson.M
	if err := cursor.All(context.Background(), &documents); err != nil {
		return nil, err
	}

	totalItems, err := collection.CountDocuments(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}

	totalPages := int(math.Ceil(float64(totalItems) / float64(perPageInt)))
	currentDateTime := time.Now().Format("2006-01-02 : 15:04:05")

	finalResponse := CustomResponse{
		Status: "ok",
		Response: map[string]interface{}{
			"items":       documents,
			"total_items": totalItems,
			"total_pages": totalPages,
		},
		Date: currentDateTime,
		APP:  "3.0.0",
	}

	return &finalResponse, nil
}

func (h *LogsHandler) perPageItem(collection *mongo.Collection, perPage string) (int, int, error) {
	var perPageInt int

	if perPage == "" {
		totalItems, err := collection.CountDocuments(context.Background(), bson.D{})
		if err != nil {
			return 0, 0, err
		}
		perPageInt = int(totalItems)
	} else {
		var err error
		perPageInt, err = strconv.Atoi(perPage)
		if err != nil {
			return 0, 0, fmt.Errorf("Invalid per_page value")
		}
		if perPageInt <= 0 {
			return 0, 0, fmt.Errorf("Invalid per_page value")
		}
	}

	return perPageInt, 0, nil
}
