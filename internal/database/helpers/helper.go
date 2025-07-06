package helpers

import (
	"advertisement-management/internal/config"
	"advertisement-management/internal/database/tables"
	"advertisement-management/internal/dtos"
	"context"
)

func GetAds(ctx context.Context, page int, limit int) ([]tables.Ads, error) {
	db := config.DB.WithContext(ctx)
	var ads []tables.Ads
	if page > 0 {
		db = db.Offset((page - 1) * limit)
	}
	if limit > 0 {
		db = db.Limit(limit)
	}
	err := db.Find(&ads).Error
	if err != nil {
		return nil, err
	}
	return ads, nil
}

func GetAd(ctx context.Context, adId int, page int, limit int) (tables.Ads, error) {
	db := config.DB.WithContext(ctx)
	var ad tables.Ads
	err := db.Where("id = ?", adId).First(&ad).Error
	if err != nil {
		return tables.Ads{}, err
	}
	return ad, nil
}

func GetTotalAds(ctx context.Context) (int, error) {
	db := config.DB
	var total int64
	err := db.WithContext(ctx).Model(&tables.Ads{}).Count(&total).Error
	if err != nil {
		return 0, err
	}
	return int(total), nil
}

func SaveClick(ctx context.Context, request dtos.SaveClickRequest) error {
	db := config.DB.WithContext(ctx)
	err := db.Create(&tables.AdsClick{
		AdsID:     request.AdID,
		IPAddress: request.IPAddress,
		VideoTime: request.VideoTime,
		Timestamp: request.Timestamp,
	}).Error
	if err != nil {
		return err
	}
	return nil
}

func GetAdCount(ctx context.Context) (int, error) {
	db := config.DB.WithContext(ctx)
	var count int64
	err := db.Model(&tables.Ads{}).Count(&count).Error
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

func GetClickCount(ctx context.Context) (int, error) {
	db := config.DB.WithContext(ctx)
	var count int64
	err := db.Model(&tables.AdsClick{}).Count(&count).Error
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

func GetClicks(ctx context.Context, adIds []int, page int, limit int) ([]tables.AdsClick, error) {
	db := config.DB.WithContext(ctx)
	var clicks []tables.AdsClick
	if page > 0 {
		db = db.Offset((page - 1) * limit)
	}
	if limit > 0 {
		db = db.Limit(limit)
	}
	if len(adIds) > 0 {
		db = db.Where("ads_id IN (?)", adIds)
	}
	err := db.Find(&clicks).Error
	if err != nil {
		return nil, err
	}
	return clicks, nil
}
