An example of Kafka Streams

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
