#!/bin/bash

# low datetime
arg1="$1"
arg2="$2"
arg3="$3"

echo "Received arguments:"
echo "arg1: $arg1"
echo "arg2: $arg2"
echo "arg3: $arg3"

# Set the path
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=~/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# set Spark path
SPARK_HOME=~/spark
# execute spark-submit command
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.mongodb:mongodb-driver-sync:4.8.1 \
  ~/BigData/middleware/mongo_read.py \
  $arg1 $arg2 $arg3

# check for errors in the spark-submit command
if [ $? -ne 0 ]
then
  echo "Spark job failed!"
  exit 1
fi

echo "Spark job submitted successfully."

