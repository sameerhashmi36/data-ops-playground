# Hadoop Storage Vault (hadoop-storage)

This repository contains the infrastructure setup for running a lightweight local Hadoop HDFS cluster using Docker containers.

The environment is designed to simulate a real-world distributed storage layer commonly used in modern data engineering systems. Instead of storing processed files directly on a local machine, the setup distributes and manages storage through Hadoop’s distributed file system architecture.

---

# Overview

The Hadoop cluster acts as the storage layer for the streaming analytics pipeline.

Processed weather data generated from Apache Kafka and PySpark Structured Streaming is converted into Parquet format and persisted inside HDFS (Hadoop Distributed File System).

This architecture mirrors enterprise-scale data lake storage systems where compute and storage are separated across distributed machines.

---

# Core Components

## NameNode (`hadoop-namenode`)

The NameNode is the central coordinator of the Hadoop filesystem.

It does not store the actual file contents. Instead, it:

- Maintains metadata about all files and folders
- Tracks where data blocks are physically stored
- Coordinates read and write operations across the cluster

Think of it as the filesystem’s master index.

---

## DataNode (`hadoop-datanode`)

The DataNode stores the actual data blocks assigned by the NameNode.

Responsibilities include:

- Persisting file blocks on disk
- Serving data during reads
- Receiving new blocks during writes
- Reporting storage health back to the NameNode

In distributed environments, multiple DataNodes work together to provide scalability and redundancy.

---

## HDFS (Hadoop Distributed File System)

HDFS combines storage from all participating machines into a single virtual filesystem.

Key characteristics:

- Distributed storage architecture
- Fault-tolerant block management
- High-throughput read/write operations
- Optimized for large-scale analytics workloads

For this local setup, replication is intentionally reduced to one copy because the environment runs on a single machine.

---

# Data Ingestion Lifecycle

The complete processing flow works as follows:

1. PySpark Structured Streaming consumes weather metrics from Apache Kafka.
2. Spark processes and batches the incoming events.
3. The processed records are converted into Parquet files.
4. Spark writes the files to HDFS over port `9000`.
5. Hadoop stores the data blocks inside persistent Docker-mounted directories.

Because the storage directories are mounted onto the local machine, data remains persistent even when containers are restarted.

---

# Project Structure

```text
hadoop-storage/
│
├── docker-compose.yml
├── hadoop.env
├── data/
│   ├── namenode/
│   └── datanode/
└── README.md
```

---

# Infrastructure Files

## `docker-compose.yml`

Defines:

- Hadoop container services
- Network configuration
- Storage volume mappings
- Port exposure
- Container dependencies

---

## `hadoop.env`

Contains Hadoop-specific environment variables and filesystem settings.

Typical customizations include:

- Replication factor configuration
- Namenode and datanode tuning
- Single-node development overrides

---

## `data/`

Persistent storage directory mapped between Docker containers and the host machine.

This folder preserves HDFS metadata and block storage between container restarts.

---

# Cluster Initialization Guide

## 1. Reset Existing Storage (One-Time Clean Setup)

Before initializing Hadoop for the first time, clear any stale metadata and format the filesystem.

```bash
# Stop all running containers
docker compose down

# Remove existing storage metadata
rm -rf data/namenode/* data/datanode/*

# Format the Hadoop filesystem
docker compose run --rm namenode hdfs namenode -format
```

This prepares a clean HDFS namespace for operation.

---

## 2. Start the Cluster

Launch both the NameNode and DataNode containers in detached mode.

```bash
docker compose up -d
```

---

## 3. Verify Cluster Health

Allow the Java services approximately 10–15 seconds to initialize.

Then run:

```bash
docker ps -a
```

Both containers should display an `Up` status:

- `hadoop-namenode`
- `hadoop-datanode`

---

# Working with HDFS

HDFS is a network-based filesystem and cannot be accessed using normal Linux filesystem commands directly from the host machine.

Instead, filesystem operations must be executed through Hadoop commands inside the container environment.

---

# Common HDFS Commands

## Create a directory for streaming output

```bash
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /weather_analytics
```

---

## Grant write permissions

Useful when PySpark requires unrestricted write access during streaming jobs.

```bash
docker exec -it hadoop-namenode hdfs dfs -chmod 777 /weather_analytics
```

---

## List root directory contents

```bash
docker exec -it hadoop-namenode hdfs dfs -ls /
```

---

## View generated Parquet files

```bash
docker exec -it hadoop-namenode hdfs dfs -ls -h /weather_analytics/parquet_data
```

---

# Hadoop Web Interface

Hadoop provides a built-in browser-based dashboard for monitoring cluster activity and filesystem state.

Open the following URL in your browser:

```text
http://localhost:9870
```

The dashboard allows you to:

- Monitor cluster storage utilization
- Inspect connected DataNodes
- Browse filesystem directories
- Verify uploaded Parquet files
- Check system health and block allocation

---

# Typical Integration Workflow

This Hadoop layer is designed to integrate with:

- Apache Kafka
- PySpark Structured Streaming
- Docker-based data engineering environments
- Local analytics and experimentation pipelines

It serves as the persistent storage backend for streaming analytics workloads.

---

# Technologies Used

- Hadoop HDFS
- Docker
- Docker Compose
- Apache Spark
- PySpark Structured Streaming
- Apache Kafka
- Parquet File Format
- Java

---

<!-- # Purpose of This Project

This setup is intended for:

- Learning distributed storage concepts
- Practicing data engineering workflows
- Building local streaming analytics environments
- Understanding HDFS architecture
- Experimenting with Spark-to-Hadoop integrations

It provides a lightweight, reproducible environment for working with distributed storage systems locally before scaling to cloud or enterprise infrastructure. -->
