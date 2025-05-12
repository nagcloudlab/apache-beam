package com.example.day8;

import com.example.model.Transaction;
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

public class AmountSumPerAccountPerWindow {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create()
        );

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("group.id", "bean-pipeline-consumer-group-10");
        consumerProps.put("auto.offset.reset", "earliest");

        // Step 1: Read from Kafka with log-append time
        var kafkaRecords = pipeline.apply("ReadFromKafka", KafkaIO.<String, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("transactions")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(consumerProps)
//                .withTimestampPolicyFactory((tp, prev) ->
//                        TimestampPolicyFactory.<String, String>withLogAppendTime().createTimestampPolicy(tp, prev))
        );

        // Step 2: Extract KV from KafkaRecord
        var kvRecords = kafkaRecords.apply("ExtractKV", MapElements
                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via((KafkaRecord<String, String> record) -> record.getKV()));

        // Step 3: Parse JSON to Transaction object
        var transactions = kvRecords.apply("ParseJson", ParDo.of(JsonToTransactionTransform.of()));

        // Step 4: Apply 15-second fixed window
        var windowedTxns = transactions.apply("Window15s", Window.into(FixedWindows.of(Duration.standardSeconds(15))));

        // Step 5: Filter out transactions with null fromAccount
        var filtered = windowedTxns.apply("FilterNullFromAccount", Filter.by(txn -> txn.getFromAccount() != null));


        // Step 5: Map to KV<fromAccount, amount>
        var keyedByAccount = filtered.apply("KeyByFromAccount", MapElements
                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                .via(txn -> KV.of(txn.getFromAccount(), txn.getAmount())));

        // Step 6: Sum per account in window
        var summed = keyedByAccount.apply("SumAmountPerAccount", Sum.doublesPerKey());

        // Step 7: Print results
        summed.apply("PrintResults", ParDo.of(new DoFn<KV<String, Double>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                KV<String, Double> entry = c.element();
                System.out.println("FromAccount: " + entry.getKey() +
                        " | Total Amount: " + entry.getValue() +
                        " | Window timestamp: " + c.timestamp());
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
