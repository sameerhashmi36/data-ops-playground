# Apache Kafka Playground

This sub-folder serves as the entry-point sandbox for understanding **Event-Driven Architecture** and real-time data streaming using Apache Kafka and Python.

---

## The Architecture

This setup simulates a multi-stream data pipeline running locally on a single machine:

* **Infrastructure:** A single Apache Kafka broker running inside a lightweight **Docker** container (operating in KRaft mode, meaning it doesn't require Zookeeper).
* **Producers (Senders):** Two independent Python background workers simulating live telemetry feeds:
    * `sender_weather.py`: Broadcasts live weather sensor data to the `weather_topic`.
    * `sender_stocks.py`: Broadcasts mock market transaction logs to the `stock_topic`.
* **Consumer (Receiver):** A multi-topic subscriber (`receiver.py`) that acts as an ingestion brain, pulling data from both streams simultaneously in real-time.

---

## How to Run the Pipeline

Follow these quick steps to get the real-time data streaming layout flashing on your screen.

### 1. Fire up the Infrastructure
Ensure Docker Desktop is open and running on your machine. Open a terminal and run the official modern Kafka image:
```bash
docker run -d --name my-kafka -p 9092:9092 apache/kafka:latest