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

- Configure Java Dependencies

```
sudo apt install openjdk-17-jdk openjdk-17-jre
sudo update-alternatives --config java
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

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

## MongoDB Installation

- Database Installation

```
sudo apt-get install gnupg curl

curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor

echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list

sudo apt-get update

sudo apt-get install -y mongodb-org

echo "mongodb-org hold" | sudo dpkg --set-selections
echo "mongodb-org-database hold" | sudo dpkg --set-selections
echo "mongodb-org-server hold" | sudo dpkg --set-selections
echo "mongodb-mongosh hold" | sudo dpkg --set-selections
echo "mongodb-org-mongos hold" | sudo dpkg --set-selections
echo "mongodb-org-tools hold" | sudo dpkg --set-selections
```

- Start , Enable and Stop MongoDB

```
# Start MongoDB
sudo systemctl start mongod
sudo systemctl status mongod
sudo systemctl enable mongod
# Validate
mongosh
# Stop MongoDB
sudo systemctl stop mongod
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
