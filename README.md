```
sudo docker run -d -p 9092:9092 --name broker apache/kafka:latest
sudo docker exec --workdir /opt/kafka/bin -it broker sh
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic sai
./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sai
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sai --from-beginning
sudo docker rm -f broker
```

