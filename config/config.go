package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	// Existing fields...
	MongoURI           string
	LogDBName          string
	LogDBCompTable     string
	AppDBName          string
	AppDBTableImp      string
	AppDBTableCli      string
	AppDBTableTimezone string
	AppDBTableImpTest  string
	AppDBTableCliTest  string
	DirPath            string
	SubDirName         string
	TempLogFileName    string
	RedisAddr          string
	RedisPassword      string
	RedisDB            int

	// New field for server port
	ServerPort string
}

func LoadConfig() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		return nil, err
	}

	redisDB, err := strconv.Atoi(os.Getenv("REDIS_DB"))
	if err != nil {
		redisDB = 0
	}

	// Get port with default value
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8086" // Default port if not specified
	}

	return &Config{
		// Existing fields...
		MongoURI:           os.Getenv("DB_URL"),
		LogDBName:          os.Getenv("LOG_DB_NAME"),
		LogDBCompTable:     os.Getenv("LOG_DB_COMP_TABLE"),
		AppDBName:          os.Getenv("APP_DB_NAME"),
		AppDBTableImp:      os.Getenv("APP_DB_TABLE_IMP"),
		AppDBTableCli:      os.Getenv("APP_DB_TABLE_CLI"),
		AppDBTableTimezone: os.Getenv("APP_DB_TABLE_TIMEZONE"),
		AppDBTableImpTest:  os.Getenv("APP_DB_TABLE_IMP_TEST"),
		AppDBTableCliTest:  os.Getenv("APP_DB_TABLE_CLI_TEST"),
		DirPath:            os.Getenv("DIRPATH"),
		SubDirName:         os.Getenv("SUBDIRNAME"),
		TempLogFileName:    os.Getenv("TEMPLOGFILENAME"),
		RedisAddr:          os.Getenv("REDIS_ADDR"),
		RedisPassword:      os.Getenv("REDIS_PASSWORD"),
		RedisDB:            redisDB,

		// New field
		ServerPort: port,
	}, nil
}
