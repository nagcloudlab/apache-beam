package com.example.day8;

import com.example.model.Transaction;
import com.example.transforms.JsonToTransactionTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.joda.time.Duration;
import org.joda.time.Instant;

import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;



import java.util.HashMap;
import java.util.Map;

public class TransactionCountPerWindow {
    public static void main(String[] args) {
        // Step 1: Create the pipeline
        Pipeline pipeline = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create()
        );

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("group.id", "bean-pipeline-consumer-group-7");
        consumerProps.put("auto.offset.reset", "earliest");

        // Step 2: Read from Kafka topic (KafkaRecord<String, String> to get event-time from metadata)
        var kafkaRecords = pipeline.apply("ReadFromKafka", KafkaIO.<String, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("transactions")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(consumerProps)
//                .withTimestampPolicyFactory((org.apache.kafka.common.TopicPartition tp, java.util.Optional<org.joda.time.Instant> previousWatermark) ->
//                        org.apache.beam.sdk.io.kafka.TimestampPolicyFactory.<String, String>withLogAppendTime()
//                                .createTimestampPolicy(tp, previousWatermark)) // uses Kafka record timestamp
        );

        // Step 3: Extract KV<String, String> from KafkaRecord
        var kvRecords = kafkaRecords.apply("ExtractKV", MapElements
                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via((KafkaRecord<String, String> record) -> record.getKV()));

        // Step 4: Convert JSON string to Transaction object
        var transactions = kvRecords.apply("JsonToTransaction", ParDo.of(JsonToTransactionTransform.of()));

        // Step 5: Apply 15-second fixed window
        var windowedTxns = transactions.apply("Window15s", Window.into(FixedWindows.of(Duration.standardSeconds(15))));

        // Step 6: Count transactions in each window
        var txnCount = windowedTxns
                .apply("CountPerWindow", Combine.globally(Count.<Transaction>combineFn()).withoutDefaults());

        // Step 7: Print the count and window time
        txnCount.apply("PrintCount", ParDo.of(new DoFn<Long, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println("Transaction Count: " + c.element() +
                        " | Window timestamp: " + c.timestamp());
            }
        }));

        // Step 8: Run pipeline
        pipeline.run().waitUntilFinish();
    }
}
