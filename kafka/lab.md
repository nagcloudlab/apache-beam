



start zookeeper
```bash
./bin/zookeeper-server-start.sh ./config/zookeeper.properties
```

start kafka
```bash
./bin/kafka-server-start.sh ./config/server.properties
```
create topic
```bash
./bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```