package com.example.day8;

import com.example.model.Transaction;
import com.example.model.TransactionType;
import com.example.transforms.JsonToTransactionTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

import java.util.HashMap;
import java.util.Map;

public class TransactionCountByTypePerWindow {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create()
        );

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("group.id", "bean-pipeline-consumer-group-8");
        consumerProps.put("auto.offset.reset", "earliest");

        // Step 1: Read from Kafka with record timestamp
        var kafkaRecords = pipeline.apply("ReadFromKafka", KafkaIO.<String, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("transactions")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(consumerProps)
//                .withTimestampPolicyFactory((tp, previous) ->
//                        TimestampPolicyFactory.<String, String>withLogAppendTime().createTimestampPolicy(tp, previous))
        );

        // Step 2: Extract KV<String, String> from KafkaRecord
        var kvRecords = kafkaRecords.apply("ExtractKV", MapElements
                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via((KafkaRecord<String, String> record) -> record.getKV()));

        // Step 3: Convert to Transaction
        var transactions = kvRecords.apply("JsonToTransaction", ParDo.of(JsonToTransactionTransform.of()));

        // Step 4: Apply 15-second fixed window
        var windowedTxns = transactions.apply("Window15s", Window.into(FixedWindows.of(Duration.standardSeconds(15))));

        // Step 5: Map to KV<TransactionType, Long>
        var keyedByType = windowedTxns.apply("KeyByTransactionType", MapElements
                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via(txn -> KV.of(txn.getType().name(), "1")));

        // Step 6: Count by TransactionType
        var countByType = keyedByType
                .apply("CountPerType", Count.perKey());

        // Step 7: Print results
        countByType.apply("PrintResults", ParDo.of(new DoFn<KV<String, Long>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                KV<String, Long> entry = c.element();
                System.out.println("TransactionType: " + entry.getKey()
                        + " | Count: " + entry.getValue()
                        + " | Window timestamp: " + c.timestamp());
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
