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

Considering that attached config files will be used

![alt text](https://github.com/mathew-i/SFCrimeStatisticsSparkStreaming/blob/master/img/screen1d.PNG)
