package com.example;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

public class ProducerClient {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        // Safe + high throughput -

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "topic1";
        List<String> keys = List.of("key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10");
        List<String> regions = List.of("chennai", "bangalore", "mumbai", "delhi", "kolkata");
        for (int i = 0; i < 10 * 1000; i++) {
            String key = keys.get(i % keys.size());
            String value = "value" + i;
            Header header = new org.apache.kafka.common.header.internals.RecordHeader("region",
                    regions.get(i % regions.size()).getBytes());
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            record.headers().add(header);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("Error sending message: " + exception.getMessage());
                } else {
                    System.out.println("Sent message with key " + key + " to topic " + metadata.topic() +
                            " partition " + metadata.partition() + " offset " + metadata.offset());
                }
            });

        }
        producer.flush();
        producer.close();
        System.out.println("Producer closed");

    }
}