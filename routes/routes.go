package routes

import (
	"app-logs/handlers"

	"github.com/gin-gonic/gin"
)

// SetupRouter sets up all the routes for the application
func SetupRouter(logsHandler *handlers.LogsHandler, appsHandler *handlers.AppsHandler, cappingHandler *handlers.CappingHandler) *gin.Engine {
	// Set up Gin router
	router := gin.Default()

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
	router.GET("/impression_by_company", appsHandler.GetImpressionByCompany)

	// Capping routes
	router.GET("/update-capping-count", cappingHandler.UpdateCappingCount)
	router.GET("/update-capping-status", cappingHandler.UpdateCappingStatus)

	return router
}
