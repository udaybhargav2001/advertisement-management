package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"advertisement-management/internal/consumers"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables
	err := godotenv.Load(".env")
	if err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	// Initialize config
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	log.Println("Starting Kafka consumers...")

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a channel to receive OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start consumers in separate goroutines
	go consumers.StartAdClickConsumer(brokers)
	go consumers.StartAdEventConsumer(brokers)

	// Example of using context-based consumer
	go func() {
		consumerConfig := consumers.ConsumerConfig{
			Topics:     []string{"custom-topic"},
			GroupID:    "custom-consumer-group",
			AutoCommit: true,
			Offset:     "latest",
			Brokers:    brokers,
		}

		// Custom message handler
		handler := func(msg *kafka.Message) error {
			log.Printf("Received message: Topic=%s, Key=%s, Value=%s",
				*msg.TopicPartition.Topic, string(msg.Key), string(msg.Value))
			return nil
		}

		consumers.StartConsumerWithContext(ctx, consumerConfig, handler)
	}()

	// Wait for shutdown signal
	<-sigChan
	log.Println("Received shutdown signal, stopping consumers...")

	// Cancel context to stop consumers
	cancel()

	log.Println("All consumers stopped")
}
