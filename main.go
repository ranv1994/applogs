package main

import (
	"log"

	"github.com/gin-gonic/gin"

	"app-logs/config"
	"app-logs/db"
	"app-logs/handlers"
	"app-logs/routes"
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

	// Initialize Redis connection (will fallback to memory cache if Redis is unavailable)
	redisDB, err := db.NewRedis(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Fatal("Error initializing cache:", err)
	}
	defer redisDB.Close()

	// Initialize MySQL connection
	mysqlDB, err := db.NewMySQL(cfg.MySQLDSN)
	if err != nil {
		log.Fatal("Error connecting to MySQL:", err)
	}
	defer mysqlDB.Close()

	// Initialize handlers
	logsHandler := handlers.NewLogsHandler(cfg, mongodb, redisDB)
	appsHandler := handlers.NewAppsHandler(cfg, mongodb, redisDB, mysqlDB)
	cappingHandler := handlers.NewCappingHandler(cfg, mongodb, redisDB, mysqlDB)

	// Setup router with all routes
	router := routes.SetupRouter(logsHandler, appsHandler, cappingHandler)

	// Start the server with configured port
	if err := router.Run("0.0.0.0:" + cfg.ServerPort); err != nil {
		log.Fatal("Error starting server:", err)
	}
}
