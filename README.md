## Kafka Installation

- Download files from : https://www.apache.org/dyn/closer.cgi?path=/kafka/3.7.0/kafka_2.13-3.7.0.tgz

- Extract the files and make a new directory containing them

```
tar -xzf kafka_2.13-3.7.0.tgz
mv kafka_2.13-3.7.0 ~/kafka
```

- Run Kafka algonside ZooKeeper

```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

- Create a topic

```
bin/kafka-topics.sh --create --topic TopicName --bootstrap-server localhost:9092
```

## Spark Installation

- Download files from : https://spark.apache.org/downloads.html

- Extract the files and make a new directory containing them

```
tar -xvf spark*
mv spark-3.5.1-bin-hadoop3 ~/spark
```

- Set up the PATH variable for temporary use (every time you close the terminal , you must modify the path)

```
export SPARK_HOME=~/spark  
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

- Create a python virtual environment and install pyspark

```
sudo apt install python3-venv
python3 -m venv .venv_name
# open virtual environment
source .venv_name/bin/activate
pip install pyspark
```

## Start using bash scripts

```
# for kafka streaming only
sudo chmod +x kafka_start.sh
./kafka_start.sh
```

```
# for kafka to mongo streaming
sudo chmod +x mongo_start.sh
./mongo_start.sh
```
