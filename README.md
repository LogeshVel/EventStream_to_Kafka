# EventStream_to_Kafka
Kafka Producer produces data from EventStream (SSE - Server Sent Events) and publish to the Topic and the Kafka Consumers can consume that messages

Using the Python the client library ```sseclient``` to iterate over the http Server Sent Event (SSE) streams (also known as EventSource, after the name of the Javascript interface inside browsers).

## Installation

```pip install sseclient```

## Demo

Start the Zookeeper and the Kafka Server

```
zookeeper-server-start.sh ~/Desktop/kafka_2.13-3.2.1/config/zookeeper.properties

kafka-server-start.sh ~/Desktop/kafka_2.13-3.2.1/config/server.properties
```

**_Producer_**

```python3 kafka_producer.py``` gets the data from the [Wikimedia EventStream](https://stream.wikimedia.org/v2/stream/recentchange) and publish the message to the Kafka Topic **wikimedia.recentchange**

**_Consumer_**

```python3 kafka_consumer.py wikimedia.recentchange cg1``` consumes the message from the topic **wikimedia.recentchange** and the consumer is in the Consumer Group **cg1**.


![image](https://user-images.githubusercontent.com/69865283/190894404-4162a4a7-a11b-47a1-a1bc-8038a112b49b.png)

![image](https://user-images.githubusercontent.com/69865283/190894448-fd38690e-ca6c-4593-b5a0-767b699580b6.png)

The Producer will produce lots of data since it is reading from the EventSource.




