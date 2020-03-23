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
### data_stream.py
To start structured streaming, execute command:
```
python data_stream.py
```
***Implementation***

To make starting program easier I've added following lines. It allows to run code with simple command python data_stream.py.
```
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 pyspark-shell'
```
After reading stream from kafka topic, I'm pulling value (which is json object produced by kafka_server.py) and timestamp (I find it most convenient real time date).
```
kafka_df = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS timestamp)")
```
For aggregation I'm using watermark and time window as it can be seen below.
```
agg_df = distinct_table.select('call_date_time', 'original_crime_type_name', 'disposition') \
    .withWatermark('call_date_time', '1 day') \
    .groupBy(
             psf.window('call_date_time', "5 minutes", "2 minutes"),
             'original_crime_type_name', 
             'disposition') \
    .count()
```
The results can be seen on the screen below:

![alt text](https://github.com/mathew-i/SFCrimeStatisticsSparkStreaming/blob/master/img/screen1d.PNG)
In the join, there are few possibilities, because it's a join of stream with static data frame.
There are two possibilities to join these data sets and it's inner join and left outer join. I was playing with those two types of joines and it can be easily changed by typing inner instead of left_outer.
```
join_query = agg_df.join(radio_code_df, radio_code_df.disposition==agg_df.disposition, "left_outer")
```
The result of the join can be seen on the screen below.
![alt text](https://github.com/mathew-i/SFCrimeStatisticsSparkStreaming/blob/master/img/screen2d.PNG)

### consumer_server.py
One of the project goals was to write plain vanilla kafka consumer. The result can be seen below.
![alt text](https://github.com/mathew-i/SFCrimeStatisticsSparkStreaming/blob/master/img/screen4.PNG)
