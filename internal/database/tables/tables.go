package tables

import "gorm.io/gorm"

type Ads struct {
	gorm.Model
	Title     string `json:"title"`
	ImageURL  string `json:"image_url"`
	TargetURL string `json:"target_url"`
}
