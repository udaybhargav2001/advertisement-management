package consumers

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// AdClickEvent represents an ad click event
type AdClickEvent struct {
	AdID      string    `json:"ad_id"`
	UserID    string    `json:"user_id"`
	Timestamp time.Time `json:"timestamp"`
	IPAddress string    `json:"ip_address"`
}

// ConsumerConfig holds consumer configuration
type ConsumerConfig struct {
	Topics     []string
	GroupID    string
	AutoCommit bool
	Offset     string // "earliest" or "latest"
	Brokers    string
}

// CreateConsumer creates and returns a Kafka consumer
func CreateConsumer(config ConsumerConfig) (*kafka.Consumer, error) {
	if config.Brokers == "" {
		config.Brokers = "localhost:9092"
	}

	if config.GroupID == "" {
		config.GroupID = "advertisement-management-consumer"
	}

	if config.Offset == "" {
		config.Offset = "latest"
	}

	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":  config.Brokers,
		"group.id":           config.GroupID,
		"auto.offset.reset":  config.Offset,
		"enable.auto.commit": config.AutoCommit,
		"session.timeout.ms": 6000,
	}

	consumer, err := kafka.NewConsumer(kafkaConfig)
	if err != nil {
		return nil, err
	}

	err = consumer.SubscribeTopics(config.Topics, nil)
	if err != nil {
		consumer.Close()
		return nil, err
	}

	log.Printf("Consumer created and subscribed to topics: %v", config.Topics)
	return consumer, nil
}

// StartAdClickConsumer starts consuming ad click events
func StartAdClickConsumer(brokers string) {
	config := ConsumerConfig{
		Topics:     []string{"ad-clicks"},
		GroupID:    "ad-click-processor",
		AutoCommit: true,
		Offset:     "earliest",
		Brokers:    brokers,
	}

	consumer, err := CreateConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Create a channel to receive OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Starting ad click consumer...")

	for {
		select {
		case sig := <-sigChan:
			log.Printf("Received signal: %v. Shutting down consumer...", sig)
			return
		default:
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Consumer error: %v", err)
				continue
			}

			// Process the message
			err = processAdClickEvent(msg)
			if err != nil {
				log.Printf("Failed to process ad click event: %v", err)
			}
		}
	}
}

// StartAdEventConsumer starts consuming general ad events
func StartAdEventConsumer(brokers string) {
	config := ConsumerConfig{
		Topics:     []string{"ad-events"},
		GroupID:    "ad-event-processor",
		AutoCommit: true,
		Offset:     "latest",
		Brokers:    brokers,
	}

	consumer, err := CreateConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Create a channel to receive OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Starting ad event consumer...")

	for {
		select {
		case sig := <-sigChan:
			log.Printf("Received signal: %v. Shutting down consumer...", sig)
			return
		default:
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Consumer error: %v", err)
				continue
			}

			// Process the message
			err = processAdEvent(msg)
			if err != nil {
				log.Printf("Failed to process ad event: %v", err)
			}
		}
	}
}

// StartConsumerWithContext starts consuming messages with context support
func StartConsumerWithContext(ctx context.Context, config ConsumerConfig, handler func(*kafka.Message) error) {
	consumer, err := CreateConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	log.Printf("Starting consumer for topics: %v", config.Topics)

	for {
		select {
		case <-ctx.Done():
			log.Println("Consumer stopped due to context cancellation")
			return
		default:
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Consumer error: %v", err)
				continue
			}

			// Process message with custom handler
			if handler != nil {
				err = handler(msg)
				if err != nil {
					log.Printf("Message handler error: %v", err)
				}
			}
		}
	}
}

// processAdClickEvent processes ad click events
func processAdClickEvent(msg *kafka.Message) error {
	var clickEvent AdClickEvent
	err := json.Unmarshal(msg.Value, &clickEvent)
	if err != nil {
		return err
	}

	log.Printf("Processing ad click event: AdID=%s, UserID=%s, Timestamp=%s, IP=%s",
		clickEvent.AdID, clickEvent.UserID, clickEvent.Timestamp.Format(time.RFC3339), clickEvent.IPAddress)

	// Add your business logic here
	// For example: update click counters, analytics, etc.

	return nil
}

// processAdEvent processes general ad events
func processAdEvent(msg *kafka.Message) error {
	var event map[string]interface{}
	err := json.Unmarshal(msg.Value, &event)
	if err != nil {
		return err
	}

	log.Printf("Processing ad event: Topic=%s, Key=%s, Value=%v",
		*msg.TopicPartition.Topic, string(msg.Key), event)

	// Add your business logic here
	// For example: update ad statistics, trigger workflows, etc.

	return nil
}
