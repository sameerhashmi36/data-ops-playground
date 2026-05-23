import os
import sys

# 1. Force Spark to pull the Kafka SQL utility package on launch
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

print("⚡ Initializing PySpark Engine with Hadoop HDFS connector capabilities...")

# 2. Build the Spark Session
spark = SparkSession.builder \
    .appName("KafkaToHDFSStream") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

# Reduce logging clutter so you can clearly see the micro-batch progress
spark.sparkContext.setLogLevel("WARN")

# 3. Define the Schema matching the sender_burst.py JSON payload
weather_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("status", StringType(), True)
])

# 4. Connect to the Live Kafka Ingestion Stream
print("🔌 Binding consumer socket to localhost:9092 ('weather_topic')...")
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_topic") \
    .option("startingOffsets", "latest") \
    .load()

# 5. Parse the raw binary payload values into Structured String Columns
parsed_stream_df = kafka_stream_df \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), weather_schema).alias("data")) \
    .select("data.*")

# 6. Stream the parsed columns into Hadoop HDFS using the Parquet storage format
# HDFS requires a 'checkpoint location' to keep track of exactly what data has been written safely.
hdfs_destination_path = "hdfs://localhost:9000/weather_analytics/parquet_data"
checkpoint_path = "hdfs://localhost:9000/weather_analytics/checkpoints"

print(f"🚀 PySpark Engine is LIVE! Redirecting streams directly to HDFS -> {hdfs_destination_path}")

query = parsed_stream_df.writeStream \
    .format("parquet") \
    .option("path", hdfs_destination_path) \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .start()

# Keep the processing thread open and listening
query.awaitTermination()