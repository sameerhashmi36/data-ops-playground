# PySpark + Kafka

A local, real-time data streaming and analytics pipeline built using Apache Kafka for message ingestion and PySpark Structured Streaming for micro-batch processing. This project demonstrates how to handle high-velocity event streams by ingesting, parsing, and displaying thousands of records per second.

---

# Architecture Overview

```text
[ sender_burst.py ] ───(5,000 JSONs/burst)───> [ Kafka Broker ] ───(Structured Streaming)───> [ spark_processor.py ] ───> Console Output
```

## Components

### Data Generator (`sender_burst.py`)
Simulates a dense IoT sensor array, pushing batches of 5,000 mock weather readings into Kafka every 5 seconds.

### Event Broker (Apache Kafka)
Acts as a fault-tolerant message queue, decoupling the data producer from the processing engine.

### Stream Processor (`spark_processor.py`)
Establishes a socket connection to Kafka, reads the raw payload bytes, applies a strict schema, and processes data using micro-batches.

---

# Step-by-Step Setup & Execution

## 1. Environment & Package Installation

To ensure complete library compatibility between PySpark, Java 21, and the Scala-compiled Kafka connector, explicit version targeting is used.

```bash
# Create and activate a clean Python environment
conda create -n test python=3.10 -y
conda activate test

# Install PySpark 3.5.1 and the Kafka Python producer client
pip install pyspark==3.5.1 kafka-python
```

---

## 2. Infrastructure Activation (Docker Kafka)

The pipeline requires a running Kafka broker.

```bash
# Start the pre-configured Kafka container
docker start my-kafka
```

---

# Execution Phase

## Step A: Initialize the Processing Engine

Launch the PySpark consumer first so it can bind to the environment and prepare to listen for incoming stream packets.

```bash
python spark_processor.py
```

### How the Connection is Established

Inside `spark_processor.py`, the connection to the message queue is declared using an explicit PySpark environment argument and a read stream configuration:

```python
import os

# Force Spark to download and use the official Kafka SQL connector dependency
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

from pyspark.sql import SparkSession

# Establish the active streaming connection to the local broker
spark = SparkSession.builder \
    .appName("KafkaWeatherStream") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_topic") \
    .load()
```

The console will display an initialization report as it resolves dependencies, followed by the activation message:

```text
 PySpark Processing Engine is Live! Waiting for Kafka packets...
```

---

## Step B: Trigger the High-Throughput Load Test

Open a separate terminal window, activate the same environment, and execute the burst sender to begin flooding the topic:

```bash
conda activate test
python sender_burst.py
```

---

# Pipeline Output Matrix

When both components are active, `sender_burst.py` prints confirmation logs indicating network transmissions:

```text
Connecting to Kafka Broker...
 Blasting high-throughput data packets into 'weather_topic'...
 Sent a burst of 5,000 records. (Total records generated: 5000)
 Sent a burst of 5,000 records. (Total records generated: 10000)
```

Simultaneously, the `spark_processor.py` terminal ingests the data packets, runs schema deserialization, and prints structured tables.

The engine automatically scales the micro-batch layout, indicating that higher volumes are successfully evaluated in the background:

```text
-------------------------------------------
Batch: 1
-------------------------------------------
+---+-----------+-------+
| id|temperature| status|
+---+-----------+-------+
|  1|       26.4|HEALTHY|
|  2|       18.1|HEALTHY|
|  3|       32.9|HEALTHY|
|  4|       22.0|HEALTHY|
|  5|       39.5|HEALTHY|
+---+-----------+-------+
only showing top 20 rows
```

The output message `only showing top 20 rows` verifies that PySpark has pulled down all 5,000 records within that specific micro-batch interval and safely queued them into the operational engine.

---

# Hadoop HDFS Storage Pipeline

After validating the real-time console stream, the pipeline can be upgraded into a persistent distributed analytics system by redirecting the processed Kafka stream into Hadoop HDFS.

Instead of printing records to the terminal, PySpark Structured Streaming continuously writes compressed Parquet blocks into distributed storage.

---

# Updated Architecture Overview

```text
                                           ┌───> [ spark_processor.py ] ──────> Console Output
                                           │
[ sender_burst.py ] ───> [ Kafka Broker ] ─┤
                                           │
                                           └───> [ spark_processor_hdfs.py ] ──> [ Hadoop HDFS ]
                                                                                           │
                                                                                           └───> [ export_to_local.py ] ───> Local CSV
```

---

# HDFS Streaming Processor (`spark_processor_hdfs.py`)

This version of the processor connects to Kafka exactly like the original stream consumer, but instead of displaying batches to the console, it writes them directly into Hadoop storage using the optimized Parquet format.

---

# Kafka + HDFS Stream Connection

Inside `spark_processor_hdfs.py`, the Spark runtime injects the Kafka connector dependency and establishes the distributed storage stream.

```python
import os

# Inject Kafka connector dependency into Spark JVM
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'
)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaToHDFSStream") \
    .getOrCreate()

# Write streaming data into Hadoop HDFS
query = parsed_stream_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/weather_analytics/parquet_data") \
    .option("checkpointLocation", "hdfs://localhost:9000/weather_analytics/checkpoints") \
    .outputMode("append") \
    .start()
```

---

# Start the HDFS Storage Processor

```bash
python spark_processor_hdfs.py
```

The terminal will initialize the Spark session and remain mostly silent afterward.

This is expected behavior because the engine is continuously compressing and committing distributed Parquet files into Hadoop storage in the background.

---

# Trigger the Kafka Load Generator

Open a second terminal window and execute the producer:

```bash
conda activate test
python sender_burst.py
```

The sender immediately begins transmitting high-throughput sensor bursts into Kafka.

---

# Distributed Storage Behavior

As the stream continues running:

- Kafka receives the incoming JSON packets
- PySpark deserializes and validates the schema
- Structured Streaming groups events into micro-batches
- Hadoop HDFS stores the output as distributed Parquet blocks
- Checkpoint metadata is continuously maintained for fault recovery

This architecture allows the engine to scale beyond console visualization and behave like a lightweight distributed analytics warehouse.

---

# HDFS Record Audit (`read_hdfs_data.py`)

To verify that records were successfully committed into distributed storage, execute:

```bash
python read_hdfs_data.py
```

Example output:

```text
📖 Reading files from HDFS path: hdfs://localhost:9000/weather_analytics/parquet_data...

📊 Total records found in HDFS storage: 60000

+-----+-----------+-------+
|   id|temperature| status|
+-----+-----------+-------+
|50169|       24.7|HEALTHY|
+-----+-----------+-------+
```

This confirms that the Parquet blocks stored inside Hadoop are readable, valid, and fully queryable through Spark.

---

# Export Distributed Data to Local CSV

Because Parquet files are distributed across multiple HDFS partitions, a utility script is included to flatten the data back into a single local CSV export.

Run:

```bash
python export_to_local.py
```


A new folder will appear locally:

```text
local_weather_export/
```

containing the final CSV dataset.

---

