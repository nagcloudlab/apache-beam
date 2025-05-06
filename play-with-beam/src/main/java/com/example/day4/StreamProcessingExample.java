package com.example.day4;

import com.fasterxml.jackson.databind.ObjectMapper;
import jdk.jfr.Description;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

public class StreamProcessingExample {

    public interface KafkaOptions extends StreamingOptions {
        @Description("Kafka bootstrap servers")
        String getBootstrapServers();

        void setBootstrapServers(String value);

        @Description("Kafka topic to read from")
        String getTopic();

        void setTopic(String value);
    }

    static class Transaction implements Serializable {
        public String postingDate;
        public String type;
        public String fromAccount;
        public String toAccount;
        public String amount;
    }

    static class ParseFn extends org.apache.beam.sdk.transforms.DoFn<KV<String, String>, Transaction> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        public static ParseFn of() {
            return new ParseFn();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String json = c.element().getValue();
            try {
                Transaction transaction = objectMapper.readValue(json, Transaction.class);
                c.output(transaction);
            } catch (Exception e) {
                System.err.println("Failed to parse JSON: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {

        // Create a pipeline options object
        KafkaOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaOptions.class);
        options.setStreaming(true);

        Pipeline pipeline = Pipeline.create(options);

        // Read from Kafka topic
        pipeline
                .apply("ReadFromKafka", KafkaIO.<String, String>read()
                        .withBootstrapServers(options.getBootstrapServers())
                        .withTopic(options.getTopic())
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withStartReadTime(org.joda.time.Instant.now().minus(Duration.standardMinutes(10)))
                        .withConsumerConfigUpdates(Map.of("enable.auto.commit", "false"))
                        .withoutMetadata()
                )
                .apply("ParseJson", ParDo.of(ParseFn.of()))
                .apply("FilterInvalidRecords", Filter.by((Transaction transaction) -> {
                    String type = transaction.type;
                    String from = transaction.fromAccount;
                    String to = transaction.toAccount;
                    String amount = transaction.amount;
                    if (type.isEmpty() || amount.isEmpty()) return false;
                    // Type-specific validation
                    if (type.equals("TRANSFER")) {
                        return !from.isEmpty() && !to.isEmpty();
                    } else if (type.equals("DEPOSIT") || type.equals("INTEREST")) {
                        return !to.isEmpty();
                    } else if (type.equals("WITHDRAWAL") || type.equals("FEE")) {
                        return !from.isEmpty();
                    }
                    return false;
                }))
                .apply("ConvertToString", MapElements.into(TypeDescriptor.of(String.class)).via(transaction -> {
                    return transaction.postingDate + "," + transaction.type + "," + transaction.fromAccount + "," + transaction.toAccount + "," + transaction.amount;
                }))
                .apply("ApplyWindowing", Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply("LogOutput", ParDo.of(new DoFn<String, Void>() {
                    @DoFn.ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println("Transaction CSV: " + c.element());
                    }
                }));
        pipeline.run(); // Let it keep running


    }
}
