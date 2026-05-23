from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("ReadHDFS").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

hdfs_path = "hdfs://localhost:9000/weather_analytics/parquet_data"

print(f"📖 Reading files from HDFS path: {hdfs_path}...")

# Read the parquet files back into a dataframe
df = spark.read.parquet(hdfs_path)

# Show the total count and the top 20 rows of data
print(f"📊 Total records found in HDFS storage: {df.count()}")
df.show()