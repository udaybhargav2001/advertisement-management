package dtos

import (
	"advertisement-management/internal/database/tables"
	"time"
)

type GetAdsRequest struct {
	Page  int `json:"page"`
	Limit int `json:"limit"`
}

type GetAdsResponse struct {
	Ads   []tables.Ads `json:"ads"`
	Total int          `json:"total"`
	Page  int          `json:"page"`
	Limit int          `json:"limit"`
}

type SaveClickRequest struct {
	AdID      int       `json:"ad_id"`
	IPAddress string    `json:"ip_address"`
	VideoTime int       `json:"video_time"`
	Timestamp time.Time `json:"timestamp"`
}

type SaveClickResponse struct {
	Message string `json:"message"`
}

type GetAnalyticsResponse struct {
	Ad tables.Ads `json:"ad"`
}

type AdAnalytics struct {
	ID               int     `json:"id"`
	Title            string  `json:"title"`
	Description      string  `json:"description"`
	ImageURL         string  `json:"image_url"`
	VideoURL         string  `json:"video_url"`
	Views            int     `json:"views"`
	ClickThroughRate float64 `json:"click_through_rate"`
	TotalClicks      int     `json:"total_clicks"`
}
