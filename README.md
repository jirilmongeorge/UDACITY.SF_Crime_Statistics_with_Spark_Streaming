# SF Crime Statistics with Spark Streaming
## UDACITY Data Streaming Nanodegree - Project II
---

### Project Overview
In this project, you will be provided with a real-world dataset, extracted from Kaggle, on San Francisco crime incidents, and you will provide statistical analyses of the data using Apache Spark Structured Streaming. You will draw on the skills and knowledge you've learned in this course to create a Kafka server to produce data, and ingest data through Spark Structured Streaming.

#### Environment
- Spark 2.4.3
- Scala 2.11.x
- Java 1.8.x
- Kafka build with Scala 2.11.x
- Python 3.6.x or 3.7.x

### How to Run the Project
__1. Kafka & Zookeeper Setup__

First, Kafka and Zookeeper servers must be running...
```sh
nohup /usr/bin/zookeeper-server-start config/zookeeper.properties > setup/startup_zk.log &
nohup /usr/bin/kafka-server-start config/server.properties > setup/startup_Kfk.log &
```

For the project, some scripts were create to automatize the setup and preparation of the environment in the `0_setup` folder.
> In the workspace, the confluent kafka and zookeeper were used.

```
sh 0_setup/00_start_servers.sh
```


__2.Run Kafka Server (Data Producer)__

Then, the Producer needs to be run in order to load data to the Kafka cluster.
This loads _SF Crime Statistics_ data taken from Kaggle. Its a json file with all the criminal incidents. (File is not included due to size of the file)

```sh
python kafka_server.py
```

For the project, some scripts were create to automatize the setup and preparation of the environment in the `1_kafka_server` folder.
```
sh 1_kafka_server/01_start_kafka_server.sh
```

> Important to validate that the messages are being written correctly in the topic.

__2.1 Validate with Kafka_Console_Consumer & Kafka_Topics__
Use the `kafka-topics` to validate that the topic was created.
```sh
kafka-topics --list --zookeeper localhost:2181
```

Use the kafka-console-consumer to validate the messages are stored in the Kafka topic.

```sh
kafka-console-consumer --bootstrap-server localhost:9092 --topic "com.udacity.police.sfo.calls" --from-beginning
```

##### Screeshot 01 - SF Crime data
![kafka-console-consumer](screenshots/screenshot01_kafka_console_consumer.png)



__2.2 Validate with Consumer Server__
Use the `consumer_server` to see the topic data. This is a custom consumer using the _confluent_kafka_python_ library. Its located on the `2_consumer_server` folder.

```sh
python consumer_server.py
```

__3. Run Spark Structured Streaming App__
Finally, run the Spark Structured Streaming application to do de aggregations.
> To change the SparkUI port to 3000 as required for the workspace, include the --conf spar.ui.port attribute

```sh
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --conf spark.ui.port=3000 data_stream.py

```

For the project, some scripts were create to automatize the setup and preparation of the environment in the `3_data_stream` folder.
```
sh 3_data_stream/02_start_data_stream.sh
```

__3.1 Validate Stream with the Progress Report & Spark UI__
#### Aggregation_Table
![agg](screenshots/screenshot02.png)
#### Progress Report
![agg](screenshots/screenshot03.png)

#### Spark UI
![agg](screenshots/sparkui.png)
> Unfortunately the streaming tab is not working on the workspace environment.
