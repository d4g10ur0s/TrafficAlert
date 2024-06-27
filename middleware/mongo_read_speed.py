# configure modules
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
# type and functions modules
from pyspark.sql.functions import col ,count, avg, countDistinct, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
# general use
import datetime as dt
import sys

# set up the query
low = sys.argv[1].split(':')
great = sys.argv[2].split(':')
gt = dt.datetime(year=2024, month=6, day=26, hour=int(low[0]), minute=int(low[1]), second=int(low[2]), tzinfo=dt.timezone.utc)
lt = dt.datetime(year=2024, month=6, day=26, hour=int(great[0]), minute=int(great[1]), second=int(great[2]), tzinfo=dt.timezone.utc)
gt = gt.isoformat()
lt = lt.isoformat()
pipeline = f'''[ {{"$match": {{"time": {{"$gt": ISODate('{gt}'), "$lt": ISODate('{lt}')}}}}}},
                 {{"$sort": {{"vcount": 1}}}}, {{"$limit": 1 }}]'''
# processed data schema
schema = StructType([
    StructField("time", TimestampType(), True),
    StructField("link", StringType(), True),
    StructField("vcount", IntegerType()),
    StructField("vspeed", DoubleType()),
    StructField("name", StringType()),
])
# set up a spark session with database
conf = SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")
sc = SparkContext(conf=conf)
spark = SparkSession.builder \
    .appName("Big Data") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/TrafficAlert") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/TrafficAlert") \
    .getOrCreate()
df = spark.read.format("mongodb")\
                 .option("database", "TrafficAlert")\
                 .option("collection", "vehicle_positions")\
                 .option("aggregation.pipeline",pipeline)\
                 .option("schema", schema)\
                 .load()
df.show()
df.printSchema()
