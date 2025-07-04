package config

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"github.com/uptrace/opentelemetry-go-extra/otelgorm"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Config struct {
	Port         string `mapstructure:"PORT"`
	DBURL        string `mapstructure:"DB_URL"`
	KafkaBrokers string `mapstructure:"KAFKA_BROKERS"`
}

var ConfigInstance *Config
var DB *gorm.DB
var KafkaProducer *kafka.Producer
var kafkaOnce sync.Once

func init() {
	godotenv.Load(".env")
	ConfigInstance = &Config{
		Port:         os.Getenv("PORT"),
		DBURL:        os.Getenv("DB_URL"),
		KafkaBrokers: os.Getenv("KAFKA_BROKERS"),
	}

	// Set default values if not provided
	if ConfigInstance.Port == "" {
		ConfigInstance.Port = ":8080"
	}
	if ConfigInstance.KafkaBrokers == "" {
		ConfigInstance.KafkaBrokers = "localhost:9092"
	}
}

func InitSQLConnection() error {
	return nil
	dsn := ConfigInstance.DBURL
	var err error
	db, err := sql.Open("nrmysql", dsn)
	if nil != err {
		panic(err)
	}
	db.SetConnMaxIdleTime(10 * time.Second)
	Db, err := gorm.Open(mysql.New(mysql.Config{Conn: db}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})

	if err != nil {
		fmt.Printf("failed to connect database: %v\n", err)
		return fmt.Errorf("failed to connect database: %v", err)
	}
	if err := Db.Use(otelgorm.NewPlugin()); err != nil {
		fmt.Println("Error in adding mysql db to otel", err)
	}

	// this adds the otel interceptor to sql calls
	if err := Db.Use(otelgorm.NewPlugin()); err != nil {
		return fmt.Errorf("Error in adding mysql db to otel %v", err)
	}

	return nil
}

func InitKafkaConnection() error {
	var initErr error

	kafkaOnce.Do(func() {
		config := &kafka.ConfigMap{
			"bootstrap.servers": ConfigInstance.KafkaBrokers,
			"client.id":         "advertisement-management-producer",
			"acks":              "all",
			"retries":           3,
			"retry.backoff.ms":  100,
		}

		producer, err := kafka.NewProducer(config)
		if err != nil {
			initErr = fmt.Errorf("failed to create Kafka producer: %v", err)
			return
		}

		KafkaProducer = producer
		log.Println("Kafka producer initialized successfully")
	})

	return initErr
}

// GetKafkaProducer returns the singleton Kafka producer
func GetKafkaProducer() *kafka.Producer {
	return KafkaProducer
}

// CloseKafkaConnection closes the Kafka producer
func CloseKafkaConnection() {
	if KafkaProducer != nil {
		KafkaProducer.Close()
		log.Println("Kafka producer closed")
	}
}
