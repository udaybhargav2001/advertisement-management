package services

import (
	"advertisement-management/internal/config"
	"advertisement-management/internal/database/helpers"
	"advertisement-management/internal/database/tables"
	"advertisement-management/internal/dtos"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

func GetAds(c *gin.Context) {
	page := c.Query("page")
	limit := c.Query("limit")
	pageInt, err := strconv.Atoi(page)
	if err != nil && page != "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "Invalid page",
		})
	} else if page == "" {
		pageInt = 0
	}
	limitInt, err := strconv.Atoi(limit)
	if err != nil && limit != "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "Invalid limit",
		})
	} else if limit == "" {
		limitInt = 10
	}
	total, err := helpers.GetTotalAds(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Error getting total ads",
		})
	}

	ads, err := helpers.GetAds(c.Request.Context(), pageInt, limitInt)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Error getting ads",
		})
	}
	c.JSON(http.StatusOK, dtos.GetAdsResponse{
		Ads:   ads,
		Page:  pageInt,
		Limit: limitInt,
		Total: total,
	})
}

func SaveClick(c *gin.Context) {
	var request dtos.SaveClickRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "Invalid request",
		})
	}

	//push message to kafka
	message, err := json.Marshal(request)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Error marshalling message",
		})
	}
	err = config.PushMsgtoTopic(config.ConfigInstance.ClickTopic, message)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Error pushing message to kafka",
		})
	}

	c.JSON(http.StatusOK, dtos.SaveClickResponse{
		Message: "Click saved successfully",
	})
}

func GetAnalytics(c *gin.Context) {
	//get ads in optimised way with cloicks and click through rate for each ad, use channels to make it concurrent
	adCount, err := helpers.GetAdCount(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Error getting ads",
		})
	}

	adsChan := make(chan tables.Ads, adCount)
	clicksChan := make(chan tables.AdsClick, adCount)
	wg := sync.WaitGroup{}

	for i := 0; i < adCount; i++ {
		wg.Add(1)
		go func(adId int) {
			defer wg.Done()
			clicks, err := helpers.GetClicks(c.Request.Context(), []int{adId}, 0, 0)
			if err != nil {
				log.Println("Error getting clicks", err)
			}
			for _, click := range clicks {
				clicksChan <- click
			}
		}(i)
		go func(adId int) {
			defer wg.Done()
			ad, err := helpers.GetAd(c.Request.Context(), adId, 0, 0)
			if err != nil {
				log.Println("Error getting ad", err)
			}
			adsChan <- ad
		}(i)
	}
	wg.Wait()
	close(adsChan)
	close(clicksChan)

	ads := []tables.Ads{}
	clicks := []tables.AdsClick{}

	for ad := range adsChan {
		ads = append(ads, ad)
	}
	for click := range clicksChan {
		clicks = append(clicks, click)
	}
	clickcount_map := make(map[int]int)
	timediff_map := make(map[int]struct {
		FirstTime time.Time
		LastTime  time.Time
	})
	wg1 := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg1.Add(1)
		go func(i int) {
			defer wg1.Done()
			for _, click := range clicks {
				if click.AdsID == i {
					clickcount_map[i]++
				}
			}
		}(i + 1)
	}
	wg1.Wait()
	wg2 := sync.WaitGroup{}
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		for _, click := range clicks {
			if _, ok := timediff_map[click.AdsID]; !ok {
				timediff_map[click.AdsID] = struct {
					FirstTime time.Time
					LastTime  time.Time
				}{FirstTime: click.Timestamp, LastTime: click.Timestamp}
			} else {
				if timediff_map[click.AdsID].FirstTime.After(click.Timestamp) {
					timediff_map[click.AdsID] = struct {
						FirstTime time.Time
						LastTime  time.Time
					}{FirstTime: click.Timestamp, LastTime: timediff_map[click.AdsID].LastTime}
				}
				if timediff_map[click.AdsID].LastTime.Before(click.Timestamp) {
					timediff_map[click.AdsID] = struct {
						FirstTime time.Time
						LastTime  time.Time
					}{FirstTime: timediff_map[click.AdsID].FirstTime, LastTime: click.Timestamp}
				}
			}
		}
	}()
	wg2.Wait()
	//do grouping and create response use map to store clicks and ads
	response := make(map[int]dtos.AdAnalytics)
	for _, ad := range ads {
		response[int(ad.ID)] = dtos.AdAnalytics{
			ID:               int(ad.ID),
			Title:            ad.Title,
			TotalClicks:      clickcount_map[int(ad.ID)],
			ClickThroughRate: float64(clickcount_map[int(ad.ID)]) / float64(timediff_map[int(ad.ID)].LastTime.Sub(timediff_map[int(ad.ID)].FirstTime).Seconds()),
		}
	}

	c.JSON(http.StatusOK, response)
}
