package handlers

import (
	services "advertisement-management/internal/services"
	"net/http"

	"github.com/gin-gonic/gin"
)

var Router *gin.Engine

func InitHandlers() {
	Router.GET("/ads", services.GetAds)
	Router.POST("/ads/click", services.SaveClick)
	Router.GET("/ads/analytics", services.GetAnalytics)
	//health check
	Router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "OK"})
	})
}
