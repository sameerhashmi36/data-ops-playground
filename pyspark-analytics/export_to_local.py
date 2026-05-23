from pyspark.sql import SparkSession

# Start a local Spark session
spark = SparkSession.builder.appName("ExportHDFSToLocal").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Read the 60,000 compressed records back out of HDFS
hdfs_path = "hdfs://localhost:9000/weather_analytics/parquet_data"
df = spark.read.parquet(hdfs_path)

# 2. Save them directly onto the laptop's local drive as a readable CSV folder
# This will create a folder named 'local_weather_export'
local_destination = "./local_weather_export"
print(f"📥 Exporting {df.count()} records from HDFS to local CSV at: {local_destination}...")

df.coalesce(1).write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(local_destination)

print("✅ Export Complete!")