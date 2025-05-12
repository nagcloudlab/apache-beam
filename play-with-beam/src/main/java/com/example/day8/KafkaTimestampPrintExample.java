package com.example.day8;

import com.example.model.Transaction;
import com.example.transforms.JsonToTransactionTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaTimestampPrintExample {
    public static void main(String[] args) {
        // Step 1: Create the pipeline
        Pipeline pipeline = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create()
        );

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("group.id", "bean-pipeline-consumer-group-2");

        // Step 2: Read from Kafka topic
        var kafkaRecords = pipeline.apply("ReadFromKafka", KafkaIO.<String, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("transactions")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(consumerProps)
                .withoutMetadata() // we don't need Kafka metadata
        );

        // Step 3: Convert JSON string into Transaction object
        var transactions = kafkaRecords.apply("JsonToTransaction", ParDo.of(JsonToTransactionTransform.of()));

        // Step 4: Print Kafka-record timestamp used by Beam
        transactions.apply("PrintTimestamp", ParDo.of(new org.apache.beam.sdk.transforms.DoFn<Transaction, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Transaction txn = c.element();
                org.joda.time.Instant timestamp = c.timestamp(); // Beam-assigned event timestamp (from Kafka record)
                System.out.println("Transaction ID: " + txn.getId()
                        + " | Beam Timestamp: " + timestamp
                        + " | Amount: " + txn.getAmount());
            }
        }));

        // Step 5: Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}
