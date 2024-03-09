from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("DGIM Algorithm") \
    .getOrCreate()

# Read data from Kafka as a stream
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .load()

# Convert binary data to string and select only the 'value' column
df = df.selectExpr("CAST(value AS STRING)")

# Start the query to display data from Kafka to console
query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
