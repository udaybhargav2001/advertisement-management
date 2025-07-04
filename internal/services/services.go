package services

import (
	"strconv"

	"github.com/gin-gonic/gin"
)

func GetAds(c *gin.Context) {
	page := c.Query("page")
	limit := c.Query("limit")
	pageInt, err := strconv.Atoi(page)
	if err != nil {
		c.JSON(400, gin.H{
			"message": "Invalid page",
		})
	}
	limitInt, err := strconv.Atoi(limit)
	if err != nil {
		c.JSON(400, gin.H{
			"message": "Invalid limit",
		})
	}

	ads := models.GetAds(pageInt, limitInt)
}
