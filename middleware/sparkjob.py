from pyspark.sql import SparkSession
from pyspark.sql.functions import col ,count, avg, countDistinct, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

import sys

# Create SparkSession
spark = SparkSession.builder \
    .appName("Vehicle Data Processing") \
    .getOrCreate()

# Setting parameters for the Spark session to read from Kafka
bootstrapServers = sys.argv[1]
subscribeType = sys.argv[2]
topics = sys.argv[3]

# desired schema
schema = StructType([
    StructField("time", StringType(), True),
    StructField("link", StringType(), True),
    StructField("v", IntegerType()),  # Change to IntegerType if v is a whole number
    StructField("name", StringType()),  # Change to DoubleType if vspeed is a decimal number
])
# data schema
json_schema = StructType([
  StructField("time", TimestampType(), True),
  StructField("link", StringType(), True),
  StructField("name", StringType(), True),
  StructField("dn", IntegerType(), True),
  StructField("orig", StringType(), True),
  StructField("dest", StringType(), True),
  StructField("t", StringType(), True),
  StructField("x", DoubleType(), True),
  StructField("s", DoubleType(), True),
  StructField("v", DoubleType(), True),
])

# Read streaming data from Kafka
df = spark \
    .readStream \
    .format("kafka")\
    .option("kafka.bootstrap.servers", bootstrapServers)\
    .option(subscribeType, topics)\
    .load()
# select raw data
raw_data = df.select(from_json(col("value").cast("string"),json_schema).alias("value"))\
       .select(*[f"value.{n}" for n in json_schema.names])
# process data to get grouped
grouped_data = raw_data.groupBy(col("time"), col("link"))\
                       .agg(avg("v").alias("vspeed"), count(col("name")).alias("vcount"))
# Replace with your desired sink configuration
query = grouped_data.writeStream \
          .outputMode("complete") \
          .format("console") \
          .start()
# Start processing streaming data
query.awaitTermination()
# Stop SparkSession
spark.stop()
