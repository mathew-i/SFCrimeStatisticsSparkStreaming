# SF Crime Statistics Spark Streaming

Real-world dataset exercise using data extracted from Kaggle on San Francisco crime incidents. Spark Structured Streaming is used for  statistical analyses. This projects main objective was to create a Kafka server to produce data, and ingest data through Spark Structured Streaming.

## Introduction
In order to run the project, you should set up following tools:
* Spark 2.4.3
* Scala 2.11.x
* Java 1.8.x
* Kafka build with Scala 2.11.x
* Python 3.6.x or 3.7.x

### Working with Kafka
Before you start Kafka, you need to start zookeeper. You can do it by executing command:
```
zookeeper-server-start config/zookeeper.properties
```
Modify config/zookeeper.properties to reflect your environment settings (i.e. free listening port number).

To start kafka execute command:
```
kafka-server-start config/server.properties
```
Modifying config/server.properties allows you to change kafka server settings like zoookeeper address, default retention period, port, replication factor, and others.

Considering that provided config files will be used, use following commands to:
* Create topic (it's not mandatory, topics are not protected, first attempt to write to topic will create it)
```
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic com.udacity.police.calls.new
```
* List existing topics
```
kafka-topics --zookeeper localhost:2181 --list
```
* Read data from topic (check if the data was produced successfuly by kafka_server.py)
```
kafka-console-consumer --bootstrap-server localhost:9092 --topic com.udacity.police.calls.new --from-beginning
```
* Write data to topic
```
kafka-console-producer --broker-list localhost:9092 --topic com.udacity.police.calls.new
```

## Producer

### kafka_server.py
Before you run the program, locate path (direct or indirect) to json data file (police-department-calls-for-service.json). Fill correct path in input_file variable.
To start producing data on topic, execute command:
```
python kafka_server.py
```
In separate terminal session execute command:
```
kafka-console-consumer --bootstrap-server localhost:9092 --topic com.udacity.police.calls.new --from-beginning
```
You should see something like below:
![alt text](https://github.com/mathew-i/SFCrimeStatisticsSparkStreaming/blob/master/img/screen3.PNG)

Produced data has following schema:
```
root
 |-- crime_id: string (nullable = true)
 |-- original_crime_type_name: string (nullable = true)
 |-- report_date: string (nullable = true)
 |-- call_date: string (nullable = true)
 |-- offense_date: string (nullable = true)
 |-- call_time: string (nullable = true)
 |-- call_date_time: string (nullable = true)
 |-- disposition: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- agency_id: string (nullable = true)
 |-- address_type: string (nullable = true)
 |-- common_location: string (nullable = true)
```
## Consumer

![alt text](https://github.com/mathew-i/SFCrimeStatisticsSparkStreaming/blob/master/img/screen1d.PNG)
