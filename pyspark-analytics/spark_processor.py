import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# 1. Download the official Kafka connector plug-in for Spark
# This tells Spark how to communicate with the Kafka network protocol
# Force Spark to use the Scala 2.12 variant of the 3.5.0 Kafka package
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

def main():
    print("Initializing PySpark Engine...")

    # 2. Initialize a Local Spark Session
    spark = SparkSession.builder \
        .appName("KafkaStreamProcessor") \
        .master("local[*]") \
        .getOrCreate()

    # Set log level to WARN to hide noisy system details
    spark.sparkContext.setLogLevel("WARN")

    # 3. Define schemas to tell Spark what our JSON data looks like
    weather_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("status", StringType(), True)
    ])

    # 4. Connect to the Live Kafka Stream
    print("Connecting PySpark to Kafka Broker (localhost:9092)...")
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "weather_topic") \
        .load()

    # 5. Transform raw binary bytes into a readable Data Table
    # Kafka sends data as a 'value' column containing binary bytes
    string_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_string")
    
    parsed_stream = string_stream \
        .withColumn("data", from_json(col("json_string"), weather_schema)) \
        .select("data.*")

    # 6. Apply some real-time logic (Example: Filter for high temperatures)
    analytics_stream = parsed_stream.filter(col("temperature") > 22.0)

    # 7. Print the resulting data table to the console in real-time
    print("🔥 PySpark Processing Engine is Live! Waiting for Kafka packets...")
    query = analytics_stream.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Keep the streaming window open running indefinitely
    query.awaitTermination()

if __name__ == "__main__":
    main()
