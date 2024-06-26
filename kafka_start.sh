#!/bin/bash

# Set the path
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=~/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
# Set Spark path (modify if different)
SPARK_HOME=~/spark
# Execute spark-submit command
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 \
  ~/BigData/middleware/sparkjob.py \
  localhost:9092 subscribe vehicle_positions

# Check for errors in the spark-submit command
if [ $? -ne 0 ]
then
  echo "Spark job failed!"
  exit 1
fi

echo "Spark job submitted successfully."
