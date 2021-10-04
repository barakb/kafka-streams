Why Kafka streams ?
===================
* Stateful stream processing
* Exactly once semantic
* Declarative topology

Key points
===========
* Kafka stream is client side lib, it run where your code is running and not on the kafka agents nodes.
* 2 entities in kafka streams, streams and tables, 2 kinds of tables local and global.
* Each of the entity can be created from the other and from kafka topics
* Default storage is rocks db, there is a built-in in memory storage and many 3rd parties, storage can be queried directly
* Streams can be manipulated using functional combinators like map, flatmap and more, each combinator 
  create a new underline kafka topic that is automatically managed and reclaim by kafka streams.
* Streams can join with table to produce stream
* Streams can window and aggregated

An example of Kafka Streams
============================
The word count example, this is how it can be run

Run kafka
```shell
docker-compose up
```

Run the example app 
```shell
./gradlew bootRun
```

Send inputs

```shell
docker-compose exec kafka bash
kafka-console-producer --bootstrap-server localhost:9092 --topic words
[type your lines here]
```

Interactive query
```shell
curl -s http://localhost:8080/api/all | jq
```
Get the values from the local storage.


How to write unit tests
========================

WordCountTests

ServiceOpsProcessingTest

UserActivitySessionTests
