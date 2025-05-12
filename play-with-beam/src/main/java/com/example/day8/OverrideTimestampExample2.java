package com.example.day8;


import com.example.model.Transaction;
import com.example.transforms.JsonToTransactionTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.HashMap;
import java.util.Map;

public class OverrideTimestampExample2 {

    public static void main(String[] args) {
        // Step 1: Create the Beam pipeline
        Pipeline pipeline = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create()
        );

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("group.id", "bean-pipeline-consumer-group-4");

        // Step 2: Read Kafka messages as KV<String, String>
        pipeline
                .apply("ReadFromKafka", KafkaIO.<String, String>read()
                        .withBootstrapServers("localhost:9092")
                        .withTopic("transactions")
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withConsumerConfigUpdates(consumerProps)
                        .withoutMetadata()
                )

                // Step 3: Convert JSON string to Transaction object
                .apply("ParseJson", ParDo.of(JsonToTransactionTransform.of()))

                // Step 4: Assign event time from record’s timestamp (handle skew)
                .apply("AssignEventTimestamps", ParDo.of(new DoFn<Transaction, Transaction>() {

                    @Override
                    public Duration getAllowedTimestampSkew() {
                        // Allow any skew into the past
                        return Duration.millis(Long.MAX_VALUE);
                    }

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Transaction txn = c.element();
                        Instant eventTime = new Instant(txn.getTimestamp().getTime());
                        System.out.println("Assigning event timestamp: " + eventTime);
                        c.outputWithTimestamp(txn, eventTime);
                    }
                }))

                // Step 5: Apply 15-second fixed window
                .apply("Window15s", Window.into(FixedWindows.of(Duration.standardSeconds(15))))

                // Step 6: Print each transaction with window timestamp
                .apply("PrintWindowedTxn", ParDo.of(new DoFn<Transaction, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println("Windowed Transaction: " + c.element()
                                + ", Timestamp: " + c.timestamp());
                    }
                }));

        // Step 7: Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}
