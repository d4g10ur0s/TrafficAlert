import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pyspark.sql.types import StructType, StructField, StringType, IntegerType , TimestampType

json_schema = StructType([
  StructField("name", StringType(), True),
  StructField("dn", StringType(), True),
  StructField("orig", StringType(), True),
  StructField("dest", StringType(), True),
  StructField("t", StringType(), True),
  StructField("link", StringType(), True),
  StructField("x", StringType(), True),
  StructField("s", StringType(), True),
  StructField("v", StringType(), True),
  StructField("time", StringType(), True),
])


if __name__ == "__main__":    # Checking validity of Spark submission command
    if len(sys.argv) != 4:
        print("Wrong number of args.", file=sys.stderr)
        sys.exit(-1)    # Initializing Spark session

    spark = SparkSession\
        .builder\
        .appName("MySparkSession")\
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # Setting parameters for the Spark session to read from Kafka
    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]

    # Streaming data from Kafka topic as a dataframe
    lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrapServers).option(subscribeType, topics)\
            .load()
    # seperate json attributes
    value_df = lines.select(from_json(col("value").cast("string"),json_schema).alias("value"))
    # unpack everything
    value_df = value_df.select(*[f"value.{n}" for n in json_schema.names])
    new_df = value_df.withColumn("time", col('time').cast(TimestampType()))\
                        .groupBy(col("time"), col("link")).count()
    query = new_df.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()

    #query = new_df\
    #        .writeStream\
    #        .outputMode("update")\
    #        .format("console")\
    #        .start().awaitTermination()
    # Writing dataframe to console in complete mode
    #query = lines\
    #        .writeStream\
    #        .format("console")\
    #        .outputMode("complete")\
    #        .start()\
    #        .awaitTermination()
    #query = value_df\
    #        .writeStream\
    #        .outputMode("append")\
    #        .format("console")\
    #        .start()    # Terminates the stream on abort
    #query.awaitTermination()
