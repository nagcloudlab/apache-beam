package com.example.day8;

import com.example.model.Transaction;
import com.example.transforms.JsonToTransactionTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Instant;

import java.util.HashMap;
import java.util.Map;

public class OverrideTimestampExample {

    public static void main(String[] args) {
        // Step 1: Create pipeline
        Pipeline pipeline = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create()
        );

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("group.id", "bean-pipeline-consumer-group-3");

        // Step 2: Read from Kafka
        var kafkaRecords = pipeline.apply("ReadFromKafka", KafkaIO.<String, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("transactions")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(consumerProps)
                .withoutMetadata()
        );

        // Step 3: Convert JSON string into Transaction object
        var transactions = kafkaRecords.apply("JsonToTransaction", ParDo.of(JsonToTransactionTransform.of()));

        // Step 4: Override Beam timestamp with Transaction.timestamp
        var reTimestamped = transactions.apply("AssignEventTimestamp", ParDo.of(new DoFn<Transaction, Transaction>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Transaction txn = c.element();
                Instant eventTime = new Instant(txn.getTimestamp().getTime()); // Convert java.util.Date to Instant
                c.outputWithTimestamp(txn, eventTime);
            }
        }));

        // Step 5: Print out new timestamp
        reTimestamped.apply("PrintCustomTimestamp", ParDo.of(new DoFn<Transaction, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Transaction txn = c.element();
                System.out.println("Custom Event Time: " + c.timestamp() +
                        " | Transaction ID: " + txn.getId() +
                        " | Amount: " + txn.getAmount());
            }
        }));

        // Step 6: Run pipeline
        pipeline.run().waitUntilFinish();
    }
}
