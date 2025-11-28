# roomStat

A data-driven IoT pipeline for real-time environmental monitoring and comfort prediction, designed to collect sensor data from Arduino-based devices, process it through a distributed streaming architecture, and provide analytics and visualization capabilities.

## Project Overview

This project implements an end-to-end IoT data pipeline that monitors room environmental conditions (temperature, humidity, light levels, and Bluetooth device presence) using Arduino sensors. The collected data flows through a message streaming layer (Kafka) via Fluent Bit, gets processed and analyzed by Apache Spark (including ML-based comfort predictions using Logistic Regression), and is stored in Elasticsearch for querying and visualization through Kibana dashboards.

The system demonstrates practical applications of distributed data engineering principles, including real-time stream processing, machine learning integration, and scalable data infrastructure—all containerized using Docker Compose for reproducibility and ease of deployment.

## Architecture Summary

```
┌─────────────┐    ┌────────────┐    ┌─────────┐    ┌─────────┐    ┌───────────────┐    ┌─────────┐
│   Arduino   │───▶│  Producer  │───▶│ Fluent  │───▶│  Kafka  │───▶│    Spark      │───▶│ Elastic │
│   Sensors   │    │  (Python)  │    │   Bit   │    │ Broker  │    │ (ML + Stream) │    │ search  │
└─────────────┘    └────────────┘    └─────────┘    └─────────┘    └───────────────┘    └────┬────┘
                                                                                              │
                                                                                              ▼
                                                                                        ┌─────────┐
                                                                                        │ Kibana  │
                                                                                        │Dashboard│
                                                                                        └─────────┘
```

**Pipeline Components:**
1. **Arduino Sensors** – Collect environmental data (temperature, humidity, light) and BLE device detection (MAC, RSSI)
2. **Producer** – Python scripts that read sensor data via serial port or from files and write to log files
3. **Fluent Bit** – Lightweight log shipper that tails log files and forwards JSON data to Kafka
4. **Kafka** – Distributed message broker for reliable data streaming
5. **Spark** – Stream processing engine with integrated ML (Logistic Regression) for comfort prediction
6. **Elasticsearch** – Stores processed data for analytics and search
7. **Kibana** – Visualization dashboards for real-time monitoring

## Repository Structure

```
roomStat/
├── arduino/                    # Arduino firmware for IoT sensors
│   ├── arduBlueScan/          # BLE scanning only
│   └── mixedData/             # Combined environmental + BLE data
├── compose-dev.yaml           # Docker Compose orchestration file
├── data/                      # Sample datasets and training data
│   ├── training.csv           # Training data for ML model
│   └── *.csv                  # Various data samples
├── fluentbit/                 # Fluent Bit configuration
│   ├── allData.conf           # Main config: tail logs → Kafka
│   ├── fluent-bit.conf        # Alternative configuration
│   └── parsers.conf           # JSON and log parsers
├── kafka/                     # Standalone Kafka configuration
│   └── compose.yaml           # Kafka-only Docker setup
├── keynote/                   # Presentation materials
├── producer/                  # Python data producers
│   ├── producer.py            # Serial port reader / file reader
│   ├── dummyProducer.py       # Direct Kafka producer
│   └── logs/                  # Log output directory
├── spark/                     # Apache Spark processing
│   ├── Dockerfile             # Custom Spark image with numpy
│   ├── python/                # PySpark scripts
│   │   ├── spark_KtoE_train_pred.py  # Main: Kafka → ML → Elasticsearch
│   │   ├── readkafka.py       # Kafka stream reader
│   │   └── training.csv       # Training data for ML model
│   └── trainingNb.ipynb       # Jupyter notebook for model training
└── utils/                     # Utility scripts
    ├── elasticExport.py       # Export data from Elasticsearch
    ├── generateDummyDataset.py # Generate synthetic training data
    └── logGen.py              # Log file generator
```

## Technologies Used

- **Arduino** – Microcontroller platform for IoT sensor data collection (BLE, temperature, humidity, light)
- **Python 3** – Producer scripts and utility tools
- **Fluent Bit** – Lightweight and fast log processor and forwarder
- **Apache Kafka** – Distributed event streaming platform for real-time data pipelines
- **Apache Spark (PySpark)** – Unified analytics engine for stream processing and ML
- **Elasticsearch** – Distributed search and analytics engine for storing processed data
- **Kibana** – Data visualization dashboard for Elasticsearch
- **Docker / Docker Compose** – Containerization and service orchestration

## How to Run the Project

### Prerequisites
- Docker and Docker Compose installed
- Python 3.x (for running producer scripts locally)
- Arduino IDE (optional, for flashing sensor firmware)

### Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/AlexMatter1512/roomStat.git
   cd roomStat
   ```

2. **Start all services with Docker Compose**
   ```bash
   docker compose -f compose-dev.yaml up -d
   ```
   This starts: Kafka broker, Fluent Bit, Spark, Elasticsearch, and Kibana.

3. **Produce sample data** (if not using Arduino hardware)
   ```bash
   cd producer
   python producer.py -d ../data/training.csv -l logs/data.log
   ```
   This reads from a sample CSV and writes to the log file monitored by Fluent Bit.

4. **Access the services**
   - **Kibana Dashboard**: http://localhost:5601
   - **Elasticsearch API**: http://localhost:9200

5. **Stop all services**
   ```bash
   docker compose -f compose-dev.yaml down
   ```

### Environment Variables
The Spark processing script supports the following environment variables:
- `BROKER` – Kafka broker address (default: `broker:9092`)
- `TOPIC` – Kafka topic name (default: `room`)
- `ELASTIC` – Elasticsearch host (default: `elasticsearch`)
- `ELASTIC_PORT` – Elasticsearch port (default: `9200`)
- `INDEX` – Elasticsearch index name (default: `room`)

## Data Flow Example

**1. Sensor Data Collection (Arduino)**
```json
{"mac": "AA:BB:CC:DD:EE:FF", "rssi": -65, "light": 150, "temperature": 24.5, "humidity": 45.2}
```

**2. Log File (Producer → Fluent Bit)**
The producer writes JSON lines to `logs/data.log`, which Fluent Bit tails.

**3. Kafka Message (Fluent Bit → Kafka)**
Fluent Bit parses the JSON and forwards it to the `room` topic in Kafka.

**4. Spark Processing (Kafka → Spark → Elasticsearch)**
Spark Structured Streaming:
- Reads from Kafka topic
- Parses JSON schema (mac, rssi, temperature, humidity, light, timestamp)
- Applies trained Logistic Regression model to predict comfort level
- Writes enriched data to Elasticsearch

**5. Final Output (Elasticsearch)**
```json
{
  "mac": "AA:BB:CC:DD:EE:FF",
  "rssi": -65,
  "temperature": 24.5,
  "humidity": 45.2,
  "light": 150,
  "prediction": 1.0,
  "prediction_str": "comfortable",
  "timestamp": "2024-06-14T10:30:00.000Z"
}
```

**6. Visualization (Kibana)**
Create dashboards to visualize comfort levels, environmental trends, and device presence over time.

## Notes for the Professor

This project demonstrates key concepts in **data engineering** and **distributed systems**:

- **IoT Data Integration**: Real-world sensor data collection using Arduino with BLE and environmental sensors
- **Stream Processing Architecture**: Implementation of a modern data pipeline with Kafka as the central message bus
- **Real-Time Analytics**: Apache Spark Structured Streaming for continuous data processing
- **Machine Learning Integration**: Logistic Regression model for binary classification (comfort prediction) trained on environmental features
- **Containerization**: Full Docker Compose setup for reproducible deployments
- **Data Visualization**: Elasticsearch + Kibana stack for analytics and monitoring
- **Scalability Considerations**: Each component can be scaled independently (Kafka partitions, Spark executors, Elasticsearch nodes)

The ML model achieves approximately **97.6% accuracy** on test data for predicting room comfort based on light, temperature, and humidity readings.

## License

This project was developed as part of a university course on data engineering and distributed systems.
