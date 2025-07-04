package models

import "advertisement-management/internal/database/tables"

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
