'''
Run with Kafka only :
~/spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 ~/BigData/middleware/spark_job.py localhost:9092 subscribe vehicle_positions
Run with Kafka and MongoDB :
~/spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.mongodb:mongodb-driver-sync:5.1.0 ~/BigData/middleware/spark_job.py localhost:9092 subscribe vehicle_positions
'''
from pyspark import SparkContext,SparkConf
# configure jars

from pyspark.sql import SparkSession
from pyspark.sql.functions import col ,count, avg, countDistinct, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql import SparkSession

import sys
# callback to write dataframe
def saveRawData(message,bid):
    message.write \
    .format("mongodb")\
    .mode("append") \
    .option("database", "TrafficAlert") \
    .option("collection", "raw_data") \
    .save()
    message.show()
    pass

# callback to write dataframe
def saveProcessedData(message,bid):
    message.write \
    .format("mongodb")\
    .mode("append") \
    .option("database", "TrafficAlert") \
    .option("collection", "vehicle_positions") \
    .save()
    message.show()
    pass

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

# set up a spark session with database
conf = SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.3.0")
sc = SparkContext(conf=conf)

spark = SparkSession.builder \
    .appName("Big Data") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/TrafficAlert") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/TrafficAlert") \
    .getOrCreate()


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
# write the raw data
tempRawData = raw_data
query = raw_data.writeStream.foreachBatch(saveRawData).start()
# process data to get grouped
grouped_data = tempRawData.withWatermark("time" , "5 seconds").groupBy(col("time"), col("link"))\
                          .agg(avg("v").alias("vspeed"), count(col("name")).alias("vcount"))
# write the processed data
query2 = grouped_data.writeStream.foreachBatch(saveProcessedData).start()
''' print data '''
#query = grouped_data.writeStream \
#          .outputMode("complete") \
#          .format("console") \
#          .start()
# Stop SparkSession
query.awaitTermination()
query2.awaitTermination()
spark.stop()
