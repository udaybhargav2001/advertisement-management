package tables

import (
	"time"

	"gorm.io/gorm"
)

type Ads struct {
	gorm.Model
	Title     string `json:"title"`
	ImageURL  string `json:"image_url"`
	TargetURL string `json:"target_url"`
}

func (*Ads) TableName() string {
	return "ads"
}

type AdsClick struct {
	gorm.Model
	AdsID     int       `json:"ads_id"`
	IPAddress string    `json:"ip_address"`
	Timestamp time.Time `json:"timestamp"`
	VideoTime int       `json:"video_time"`
}

func (*AdsClick) TableName() string {
	return "ads_click"
}
