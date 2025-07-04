package helpers

import (
	"advertisement-management/internal/config"
	"advertisement-management/internal/database/tables"
)

func GetAds(page int, limit int) []tables.Ads {
	db := config.DB
	var ads []tables.Ads
	db.Find(&ads)
	return ads
}
