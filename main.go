package main

import (
	config "advertisement-management/internal/config"
	handlers "advertisement-management/internal/handlers"
	"fmt"

	"github.com/gin-gonic/gin"
)

func main() {
	fmt.Println("Hello, World!")
	gin.SetMode(gin.ReleaseMode)

	// Initialize SQL connection
	err := config.InitSQLConnection()
	if err != nil {
		fmt.Println("Error initializing SQL connection:", err)
	}

	// Initialize Kafka connection
	err = config.InitKafkaConnection()
	if err != nil {
		fmt.Println("Error initializing Kafka connection:", err)
	}

	// Setup HTTP handlers
	handlers.Router = gin.Default()
	handlers.InitHandlers()

	// Start server
	fmt.Println("Server starting on port", config.ConfigInstance.Port)
	err = handlers.Router.Run(config.ConfigInstance.Port)
	if err != nil {
		fmt.Println("Error starting server:", err)
	}

	// Cleanup on exit
	defer config.CloseKafkaConnection()
}
