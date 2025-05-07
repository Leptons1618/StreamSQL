# StreamSQL

A real-time data streaming solution that captures database changes and forwards them to MQTT messaging systems.

## Overview

StreamSQL is a data integration pipeline that uses Change Data Capture (CDC) to monitor SQL Server database changes and streams them through Apache Kafka to MQTT brokers in real-time. This solution enables event-driven architectures and real-time analytics with minimal impact on source systems.

## Architecture

![StreamSQL Architecture](./StreamSQL+Architecture.png)

The solution consists of the following components:

- **SQL Server with CDC**: Source database with Change Data Capture enabled
- **Debezium**: Captures database changes and streams them to Kafka
- **Apache Kafka**: Message broker for reliable data streaming
- **Kafka Connect**: Framework for connecting Kafka to external systems
- **StreamSQL Bridge**: Python service that forwards messages from Kafka to MQTT
- **HiveMQ Cloud**: MQTT broker for distributing messages to client applications

## Prerequisites

- Docker and Docker Compose
- Python 3.8 or higher
- Required Python packages:
  - paho-mqtt (MQTT client for Python)
  - pykafka (Kafka client for Python)
- SQL Server with CDC enabled
- Network access to SQL Server and HiveMQ Cloud

### Python Package Installation

```bash
pip install paho-mqtt pykafka
```

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/Leptons1618/StreamSQL.git
cd streamsql
```

### 2. Configure SQL Server Connector

Edit the `mssql-source-connector.json` file with your SQL Server details:

```json
{
    "name": "mssql-source-connector",
    "config": {
        "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
        "database.hostname": "your-sql-server",
        "database.port": "1433",
        "database.user": "your-username",
        "database.password": "your-password",
        "database.dbname": "your-database",
        // Additional configuration...
    }
}
```

### 3. Configure MQTT Connection

Update the `kafka_to_hivemq_cloud.py` file with your HiveMQ Cloud credentials:

```python
MQTT_BROKER = 'your-mqtt-broker.hivemq.cloud'
MQTT_PORT = 8883
MQTT_USERNAME = 'your-username'
MQTT_PASSWORD = 'your-password'
MQTT_TOPIC = 'your-topic'
```

### 4. Start the Infrastructure

```bash
docker-compose up -d
```

### 5. Start the StreamSQL Bridge

```bash
python kafka_to_hivemq_cloud.py
```

## Configuration Options

### Kafka Connect Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `snapshot.mode` | CDC snapshot mode | `initial` |
| `topic.creation.enable` | Automatic topic creation | `true` |
| `tasks.max` | Maximum number of tasks | `1` |

### StreamSQL Bridge Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | `localhost:9092` |
| `KAFKA_TOPIC` | Source Kafka topic | `sqlserver1.dbo.Customers` |
| `auto_offset_reset` | Consumer offset strategy | `latest` |

## Monitoring and Management

- Access the Kafka UI at [http://localhost:8080](http://localhost:8080)
- Monitor Kafka Connect at [http://localhost:8083](http://localhost:8083)
- Check HiveMQ Cloud dashboard for MQTT message statistics

## Alternative Implementations

The repository includes two versions of the Kafka to MQTT bridge:

1. **kafka_to_hivemq_cloud.py**: Standard implementation using kafka-python
2. **kafkaHiveBrocker.py**: Alternative implementation using PyKafka

## Troubleshooting

### Common Issues

- **No messages flowing**: Verify SQL Server CDC is properly configured
- **Connector failures**: Check Kafka Connect logs for detailed error messages
- **MQTT connection issues**: Verify credentials and network connectivity

### Viewing Logs

```bash
docker logs streamsql_kafka-connect_1
```

## License

MIT

## Contact

For questions or support, please contact [anishgiri163@gmail.com](mailto:anishgiri163@gmail.com).
