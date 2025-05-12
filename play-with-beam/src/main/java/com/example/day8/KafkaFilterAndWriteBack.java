package com.example.day8;

import com.example.model.Transaction;
import com.example.transforms.JsonToTransactionTransform;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaFilterAndWriteBack {
    public static void main(String[] args) {
        var pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("group.id", "bean-pipeline-consumer-group-12");
        consumerProps.put("aut.offset.reset","earliest");


        // Step 1: Read from Kafka
        var kafkaInput = pipeline.apply("ReadKafka", KafkaIO.<String, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("transactions")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata()
        );

        // Step 2: Parse JSON to Transaction
        var transactions = kafkaInput
                .apply("ToTransaction", ParDo.of(JsonToTransactionTransform.of()));

        // Step 3: Filter transactions > 10,000
        var highValueTxns = transactions.apply("FilterHighAmount", Filter.by(txn -> txn.getAmount() > 1000));

        // Step 4: Convert Transaction back to JSON
        var jsonOutput = highValueTxns.apply("ToJson", MapElements.into(TypeDescriptors.strings())
                .via(txn -> {
                    ObjectMapper mapper = new ObjectMapper();
                    try {
                        return mapper.writeValueAsString(txn);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }));

        // Step 5: Create Kafka Key-Value records
        var kvOutput = jsonOutput.apply("ToKV", MapElements
                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via(json -> KV.of("key", json)));

        // Step 6: Write back to Kafka
        kvOutput.apply("WriteToKafka", KafkaIO.<String, String>write()
                .withBootstrapServers("localhost:9092")
                .withTopic("high-value-transactions")
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class)
        );

        pipeline.run().waitUntilFinish();
    }
}
