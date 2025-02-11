package main

import (
	"log"
	"os"

	"github.com/gin-gonic/gin"

	"app-logs/config"
	"app-logs/db"
	"app-logs/handlers"
)

func main() {
	// Set Gin to release mode
	gin.SetMode(gin.ReleaseMode)

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatal("Error loading config:", err)
	}

	// Initialize MongoDB connection
	mongodb, err := db.NewMongoDB(cfg.MongoURI)
	if err != nil {
		log.Fatal("Error connecting to MongoDB:", err)
	}
	defer mongodb.Close()

	// Initialize Redis connection or use dummy Redis
	var redisDB *db.Redis
	if os.Getenv("USE_REDIS") == "true" {
		redisDB, err = db.NewRedis(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
		if err != nil {
			log.Printf("Warning: Redis connection failed: %v. Using dummy cache instead.", err)
			redisDB = db.NewDummyRedis() // Create a dummy Redis implementation
		}
	} else {
		log.Println("Using dummy cache instead of Redis")
		redisDB = db.NewDummyRedis() // Create a dummy Redis implementation
	}
	defer redisDB.Close()

	// Initialize handlers
	logsHandler := handlers.NewLogsHandler(cfg, mongodb, redisDB)
	appsHandler := handlers.NewAppsHandler(cfg, mongodb, redisDB)

	// Set up Gin router
	router := gin.Default()

	// Original paths maintained
	router.GET("/savedlogs/:folder/:file", logsHandler.APIHandler)
	router.GET("/getlogs", logsHandler.APIFETCHANDLER)
	router.GET("/getAlllogs", logsHandler.APIFatchAllTokens)
	router.POST("/saveappdetails", appsHandler.SaveAppData)
	router.POST("/saveappdetails_test", appsHandler.SaveAppDataTest)
	router.GET("/impression", appsHandler.GetImpression)
	router.GET("/impression_hourly", appsHandler.HourlyGetImpression)
	router.GET("/country_timezone", appsHandler.GetTimezone)

	// Start the server with configured port
	if err := router.Run("0.0.0.0:" + cfg.ServerPort); err != nil {
		log.Fatal("Error starting server:", err)
	}
}
