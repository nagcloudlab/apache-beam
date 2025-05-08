



### start kafka cluster

```bash
./bin/kafka-storage.sh random-uuid
./bin/kafka-storage.sh format -t fC75SBQWQomVUqbD90om6w -c config/server.properties
./bin/kafka-server-start.sh config/server.properties
```


### start kafka UI
```bash
mkdir -p kafka-ui
cd kafka-ui
download kafka-ui jar
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

### start flink cluster  ( as runner for beam)
```bash
./bin/start-cluster.sh
```