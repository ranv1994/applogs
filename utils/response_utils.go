package utils

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type ResponseUtils struct{}

func NewResponseUtils() *ResponseUtils {
	return &ResponseUtils{}
}

type ErrorResponse struct {
	Status     string `json:"status"`
	Message    string `json:"message"`
	AppVersion string `json:"appVersion"`
}

func (r *ResponseUtils) ErrorResponse(c *gin.Context, message string) {
	response := ErrorResponse{
		Status:     "error",
		Message:    message,
		AppVersion: "3.0.1",
	}
	c.JSON(http.StatusOK, response)
}
