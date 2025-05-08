



### start kafka cluster

```bash
./bin/kafka-storage.sh random-uuid
./bin/kafka-storage.sh format -t 4X18eZrgSse1LpQf35qbNQ -c config/server.properties
./bin/kafka-server-start.sh config/server.properties
```


### create topic
```bash
./bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```


### start kafka UI
```bash
mkdir -p kafka-ui
cd kafka-ui
wget https://github.com/provectus/kafka-ui/releases/download/v0.7.2/kafka-ui-api-v0.7.2.jar
```

application-local.yml
```yml
kafka:
  clusters:
    - name: local
      bootstrapServers: localhost:9092
dynamic.config.enabled: true
```

run kafka-ui
```bash
java -Dspring.config.additional-location=application-local.yml -jar kafka-ui-api-v0.7.2.jar
```


### start flink cluster  ( as runner for beam)  ( optional )
```bash
./bin/start-cluster.sh
```


### start transaction-producer-client ( to kafka )
```bash
cd kafka-clients/transaction-producer
mvn clean package
java -jar target/transaction-producer-0.0.1-SNAPSHOT.jar
```