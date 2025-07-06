package consumers

import (
	"advertisement-management/internal/config"
	"advertisement-management/internal/database/helpers"
	"advertisement-management/internal/dtos"
	"context"
	"encoding/json"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func ProcessClickWorker(workerId int) {
	consumer, err := config.GetKafkaConsumer(config.ConfigInstance.ClickTopic)
	if err != nil {
		log.Println("Error creating Kafka consumer", err)
	}

	for {
		ev := consumer.Poll(100)
		if ev == nil {
			continue
		}
		switch e := ev.(type) {
		case *kafka.Message:
			msg := ev.(*kafka.Message)
			ClickEventHandler(msg)
		case kafka.AssignedPartitions:
			consumer.Assign(e.Partitions)
		case kafka.RevokedPartitions:
			consumer.Unassign()
		case kafka.Error:
			log.Println("Error", e)
		default:
			log.Println("Other event", e)
		}
	}
}

func ClickEventHandler(msg *kafka.Message) {
	var request dtos.SaveClickRequest
	if err := json.Unmarshal(msg.Value, &request); err != nil {
		log.Println("Error unmarshalling message", err)
	}

	err := helpers.SaveClick(context.Background(), request)
	if err != nil {
		log.Println("Error saving click", err)
	}
}
