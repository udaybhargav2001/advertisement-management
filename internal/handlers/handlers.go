package handlers

import (
	services "advertisement-management/internal/services"

	"github.com/gin-gonic/gin"
)

var Router *gin.Engine

func InitHandlers() {
	Router.GET("/ads", services.GetAds)
	Router.POST("/ads/click", services.SaveClick)
	Router.GET("/ads/analytics", services.GetAnalytics)
}
