# Le Son LV2 Project 01 - Kafka Streaming Pipeline

## 📋 Project Overview

This project implements a **real-time streaming data pipeline** that transfers messages between Kafka clusters and persists data to MongoDB. It provides two main pipeline types:
- **Kafka2Kafka**: Transfers messages from remote Kafka to local Kafka
- **Kafka2Mongo**: Consumes messages from Kafka and stores them in MongoDB

The solution is designed for **high-throughput, fault-tolerant data processing** with batch consumption, individual message processing, and comprehensive error handling.

## 🏗️ Architecture

```
Remote Kafka → Kafka2Kafka → Local Kafka → Kafka2Mongo → MongoDB
    (Source)      (Replication)     (Local Hub)    (Persistence)   (Storage)
```

### Key Components

- **KafkaConnector**: Manages Kafka connections (consumer, producer, admin)
- **MongoDBConnector**: Handles MongoDB operations and connections
- **StreamingPipeline**: Base class with common pipeline functionality
- **Kafka2Kafka**: Replicates data between Kafka clusters
- **Kafka2Mongo**: Streams data from Kafka to MongoDB with metadata enrichment

## ✨ Features

### 🚀 Performance
- **Batch Consumption**: Efficient message retrieval from Kafka
- **Individual Processing**: Message-by-message processing for fault tolerance
- **Configurable Batching**: Adjustable batch sizes for different scenarios
- **Smart Logging**: Configurable logging intervals to optimize performance

### 🛡️ Reliability
- **At-Least-Once Delivery**: Guarantees no data loss
- **Individual Commits**: Commits only after successful processing
- **Error Handling**: Comprehensive error handling with recovery
- **Graceful Shutdown**: Clean resource cleanup on interruption

### 🔧 Flexibility
- **Fresh Consumer Groups**: Option to consume from beginning
- **SASL Authentication**: Secure Kafka connections
- **Metadata Enrichment**: Adds Kafka metadata to MongoDB documents
- **JSON Transformation**: Automatic JSON parsing with error handling

## 📁 Project Structure

```
Le_Son_LV2_Project_01/
├── pipeline.py          # Main pipeline implementation
├── utils.py            # Custom Logger with decorators
├── config.py           # Configuration loader utility
├── database.ini        # Database connection configurations
├── kafka2kafka_run.py  # Script to run Kafka-to-Kafka pipeline
├── kafka2mongo_run.py  # Script to run Kafka-to-MongoDB pipeline
├── logs/               # Log files directory
├── notebook/           # Jupyter notebooks for testing
└── README.md           # This file
```

## 🚀 Getting Started

### Prerequisites

1. **Python Environment**
   ```bash
   python >= 3.8
   ```

2. **Required Packages**
   ```bash
   pip install confluent-kafka pymongo configparser
   ```

3. **Infrastructure**
   - Kafka cluster (remote/source)
   - Kafka cluster (local/destination) 
   - MongoDB instance

### Configuration Setup

1. **Configure database.ini**
   ```ini
   [remote_kafka]
   servers = your-remote-kafka:9092
   username = your-username
   password = your-password
   default_consumer_group_id = remote-consumer-group
   default_consumer_topic = source-topic

   [local_kafka]
   servers = your-local-kafka:9092
   username = your-username
   password = your-password
   default_consumer_group_id = local-consumer-group
   default_consumer_topic = local-topic
   default_producer_topic = local-topic

   [mongodb]
   host = localhost
   port = 27017
   default_database = glamira
   default_collection = raw
   ```

### Usage Examples

#### 1. Kafka-to-Kafka Replication

```python
from pipeline import KafkaConnector, Kafka2Kafka

# Initialize connectors
remote_kafka = KafkaConnector("remote_kafka")
local_kafka = KafkaConnector("local_kafka")

# Create and run pipeline
kafka2kafka = Kafka2Kafka(
    remote_kafka=remote_kafka,
    local_kafka=local_kafka,
    batch_size=100,        # Process 100 messages per batch
    log_interval=500       # Log progress every 500 messages
)

# Run pipeline (consume from beginning)
kafka2kafka.run(
    topic="source-topic",
    group_id="replication-group",
    consume_from_beginning=True
)
```

#### 2. Kafka-to-MongoDB Streaming

```python
from pipeline import KafkaConnector, MongoDBConnector, Kafka2Mongo

# Initialize connectors
local_kafka = KafkaConnector("local_kafka")
local_mongo = MongoDBConnector("mongodb")

# Create and run pipeline
kafka2mongo = Kafka2Mongo(
    local_kafka=local_kafka,
    local_mongo=local_mongo,
    batch_size=100,        # Process 100 messages per batch
    log_interval=1000      # Log progress every 1000 messages
)

# Run pipeline
kafka2mongo.run(
    topic="data-topic",
    group_id="mongodb-consumer",
    consume_from_beginning=False
)
```

#### 3. Using the Run Scripts

**Run Kafka2Kafka Pipeline:**
```bash
python kafka2kafka_run.py
```

**Run Kafka2Mongo Pipeline:**
```bash
python kafka2mongo_run.py
```

### Advanced Configuration

#### Performance Tuning
```python
# High-throughput scenario
pipeline = Kafka2Kafka(
    remote_kafka, local_kafka,
    batch_size=500,        # Larger batches
    log_interval=5000      # Less frequent logging
)

# Development/Testing scenario  
pipeline = Kafka2Kafka(
    remote_kafka, local_kafka,
    batch_size=10,         # Smaller batches
    log_interval=10        # Frequent logging
)
```

#### Consuming from Beginning
```python
# Start fresh and consume all historical data
pipeline.run(consume_from_beginning=True)

# Continue from last committed offset
pipeline.run(consume_from_beginning=False)
```

## 📊 Data Flow

### Kafka2Kafka Pipeline
1. **Consume**: Batch consumption from remote Kafka
2. **Transfer**: Individual message transfer to local Kafka
3. **Commit**: Individual offset commits after successful transfer

### Kafka2Mongo Pipeline
1. **Consume**: Batch consumption from local Kafka
2. **Transform**: JSON parsing and metadata enrichment
3. **Store**: Individual document insertion to MongoDB
4. **Commit**: Individual offset commits after successful storage

### Message Metadata (MongoDB)
Each document stored in MongoDB includes:
```json
{
  "original_data": "...",
  "_kafka_key": "message-key",
  "_kafka_topic": "topic-name", 
  "_kafka_partition": 0,
  "_kafka_offset": 12345,
  "_processed_at": "2025-08-09T10:30:45.123456"
}
```

## 🔧 Monitoring & Logging

### Log Levels
- **INFO**: Progress updates, batch completions, operational events
- **DEBUG**: Individual message details (enable for troubleshooting)
- **WARNING**: Unexpected but recoverable issues
- **ERROR**: Failures and exceptions

### Log Files
- Location: `logs/YYYYMMDD_HHMMSS_streaming.log`
- Rotation: New file per pipeline execution
- Format: Timestamped with logger level and message

### Key Metrics
- **Messages processed**: Total count of successful transfers/storage
- **Batch completion**: Number of batches processed
- **Error rates**: Failed JSON parsing, connection issues
- **Throughput**: Messages per second/minute

## 🛠️ Troubleshooting

### Common Issues

**1. Connection Failures**
```
ERROR: Failed to connect to Kafka/MongoDB
```
- Check `database.ini` configuration
- Verify network connectivity
- Confirm authentication credentials

**2. JSON Parsing Errors**
```
ERROR: Failed to parse JSON at offset X
```
- Check message format in source topic
- Verify data encoding (UTF-8)
- Enable debug logging for message inspection

**3. Consumer Group Issues**
```
ERROR: Consumer group rebalancing
```
- Use `consume_from_beginning=True` for fresh start
- Check consumer group conflicts
- Verify topic permissions

**4. MongoDB Insertion Failures**
```
ERROR: Failed to insert document to MongoDB
```
- Check MongoDB connection and permissions
- Verify collection exists and is accessible
- Monitor MongoDB disk space

### Debug Mode
```python
# Enable debug logging
logger = Logger(logger_level='DEBUG', log_file=log_filename)

# Use small batches for detailed inspection
pipeline = Kafka2Mongo(kafka, mongo, batch_size=1, log_interval=1)
```

## 🎯 Best Practices

### Production Deployment
1. **Set appropriate batch sizes** (100-500 for high throughput)
2. **Configure logging intervals** (1000+ for production)
3. **Monitor resource usage** (CPU, memory, network)
4. **Set up alerting** for pipeline failures
5. **Regular offset monitoring** to prevent lag

### Development Testing
1. **Use small batch sizes** (10-50 for testing)
2. **Enable debug logging** for troubleshooting
3. **Test with sample data** before production
4. **Verify error handling** with malformed messages

### Security
1. **Use SASL authentication** for Kafka connections
2. **Secure MongoDB connections** with authentication
3. **Protect configuration files** with appropriate permissions
4. **Monitor access logs** for security events

## 📈 Performance Considerations

### Throughput Optimization
- **Batch Size**: Larger batches = higher throughput, higher latency
- **Flush Strategy**: Immediate flush = lower throughput, better fault tolerance
- **Logging Frequency**: Less logging = better performance
- **Network**: Optimize for bandwidth and latency

### Memory Management
- **Batch consumption** reduces memory footprint vs. accumulating messages
- **Individual processing** prevents large in-memory collections
- **Connection pooling** reuses database connections efficiently

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add comprehensive tests
4. Update documentation
5. Submit a pull request

## 📝 License

This project is part of the Data Engineering Journey and is intended for educational and professional development purposes.

---

**Author**: Le Son  
**Project**: Level 2 Data Engineering - Real-time Streaming Pipeline  
**Last Updated**: August 2025
