package routes

import (
	"rest-api-logs/handlers"

	"github.com/gin-gonic/gin"
)

func SetupRoutes(
	router *gin.Engine,
	logsHandler *handlers.LogsHandler,
	appsHandler *handlers.AppsHandler,
) {
	// Logs routes
	router.GET("/savedlogs/:folder/:file", logsHandler.APIHandler)
	router.GET("/getlogs", logsHandler.APIFETCHANDLER)
	router.GET("/getAlllogs", logsHandler.APIFatchAllTokens)

	// Apps routes
	router.POST("/saveappdetails", appsHandler.SaveAppData)
	router.POST("/saveappdetails_test", appsHandler.SaveAppDataTest)
	router.GET("/impression", appsHandler.GetImpression)
	router.GET("/impression_hourly", appsHandler.HourlyGetImpression)
	router.GET("/country_timezone", appsHandler.GetTimezone)
}
