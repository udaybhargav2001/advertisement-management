# Advertisement Management System

A high-performance advertisement management system built with Go, providing REST APIs for serving ads, tracking clicks, and generating analytics with real-time event processing.

## 🏗️ Architecture

The system follows a microservices architecture with the following components:

- **HTTP API Server**: Gin-based REST API for serving ads and analytics
- **Event Streaming**: Kafka for asynchronous click event processing
- **Database**: MySQL with GORM ORM for data persistence
- **Background Workers**: Concurrent processing for click analytics

## 🚀 Features

- **Ad Serving**: Paginated ad retrieval with filtering
- **Click Tracking**: Asynchronous click event processing via Kafka
- **Analytics**: Real-time click-through rate calculations
- **Concurrent Processing**: Optimized performance with goroutines
- **Scalable Architecture**: Event-driven design for high throughput

## 📋 Prerequisites

- Go 1.22 or higher
- MySQL 8.0+
- Apache Kafka 2.8+
- Docker (optional)

## 🛠️ Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd advertisement-management
   ```

2. **Install dependencies**
   ```bash
   go mod download
   ```

3. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

4. **Environment Configuration**
   Create a `.env` file with the following variables:
   ```env
   PORT=:8080
   DB_URL=user:password@tcp(localhost:3306)/ads_db?charset=utf8mb4&parseTime=True&loc=Local
   KAFKA_BROKERS=localhost:9092
   CLICK_TOPIC=advertisement_clicks
   ```

## 🗄️ Database Schema

### Tables

**ads**
- `id` (Primary Key)
- `title` (String)
- `image_url` (String)
- `target_url` (String)
- `created_at`, `updated_at`, `deleted_at` (GORM timestamps)

**ads_click**
- `id` (Primary Key)
- `ads_id` (Foreign Key)
- `ip_address` (String)
- `timestamp` (DateTime)
- `video_time` (Integer)
- `created_at`, `updated_at`, `deleted_at` (GORM timestamps)

## 🏃 Running the Application

### Development Mode
```bash
# Using Makefile (with nodemon for auto-reload)
make server

# Or directly
go run main.go
```

### Production Mode
```bash
go build -o advertisement-management
./advertisement-management
```

### Using Docker
```bash
docker build -t advertisement-management .
docker run -p 8080:8080 advertisement-management
```

## 📡 API Endpoints

### 1. Get Advertisements
```http
GET /ads?page=0&limit=10
```

**Response:**
```json
{
  "ads": [
    {
      "id": 1,
      "title": "Sample Ad",
      "image_url": "https://example.com/image.jpg",
      "target_url": "https://example.com"
    }
  ],
  "total": 100,
  "page": 0,
  "limit": 10
}
```

### 2. Save Click Event
```http
POST /ads/click
Content-Type: application/json

{
  "ad_id": 1,
  "ip_address": "192.168.1.1",
  "video_time": 30,
  "timestamp": "2024-01-01T12:00:00Z"
}
```

**Response:**
```json
{
  "message": "Click saved successfully"
}
```

### 3. Get Analytics
```http
GET /ads/analytics
```

**Response:**
```json
{
  "1": {
    "id": 1,
    "title": "Sample Ad",
    "total_clicks": 150,
    "click_through_rate": 0.25
  },
  "2": {
    "id": 2,
    "title": "Another Ad",
    "total_clicks": 200,
    "click_through_rate": 0.30
  }
}
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | Server port | `:8080` |
| `DB_URL` | MySQL connection string | - |
| `KAFKA_BROKERS` | Kafka broker addresses | `localhost:9092` |
| `CLICK_TOPIC` | Kafka topic for click events | - |

### Kafka Configuration

The system uses Kafka for asynchronous click processing with the following setup:
- **Producer**: Publishes click events to the specified topic
- **Consumer**: Processes click events and saves to database
- **Configuration**: Optimized for reliability with `acks=all` and retries

## 🏗️ Project Structure

```
advertisement-management/
├── internal/
│   ├── config/          # Configuration and connections
│   ├── consumers/       # Kafka consumers
│   ├── database/
│   │   ├── helpers/     # Database operations
│   │   └── tables/      # Database models
│   ├── dtos/           # Data transfer objects
│   ├── handlers/       # HTTP route handlers
│   └── services/       # Business logic
├── main.go             # Application entry point
├── go.mod              # Go module dependencies
├── go.sum              # Dependency checksums
├── Makefile           # Build and run commands
└── README.md          # This file
```

## 📊 Performance Features

### Concurrent Processing
- **Goroutines**: Used for concurrent analytics processing
- **Channels**: Efficient data passing between goroutines
- **Wait Groups**: Proper synchronization for concurrent operations

### Optimizations
- **Batch Processing**: Efficient bulk operations
- **Connection Pooling**: Optimized database connections
- **Async Processing**: Non-blocking click event handling

## 🔨 Development

### Running Tests
```bash
go test ./...
```

### Building for Production
```bash
go build -ldflags="-s -w" -o advertisement-management
```

### Code Quality
- Follow Go best practices
- Use `go fmt` for formatting
- Run `go vet` for static analysis
- Use `golint` for linting

## 📦 Dependencies

### Core Dependencies
- **Gin**: HTTP web framework
- **GORM**: ORM for database operations
- **Confluent Kafka Go**: Kafka client
- **MySQL Driver**: Database connectivity

### Development Dependencies
- **GoDotEnv**: Environment variable management
- **OpenTelemetry**: Observability and tracing

## 🚀 Deployment

### Docker Deployment
```bash
docker build -t advertisement-management .
docker run -d -p 8080:8080 --env-file .env advertisement-management
```

## 🔍 Monitoring

### Health Checks
The application provides built-in health monitoring through:
- Database connection status
- Kafka producer/consumer health
- HTTP server status

### Metrics
- Request latency
- Error rates
- Database query performance
- Kafka message processing rates

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Commit your changes: `git commit -am 'Add new feature'`
4. Push to the branch: `git push origin feature/new-feature`
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🔗 Related Projects

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Gin Documentation](https://gin-gonic.com/docs/)
- [GORM Documentation](https://gorm.io/docs/)

---

For questions or support, please open an issue in the repository.