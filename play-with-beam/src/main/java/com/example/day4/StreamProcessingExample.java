package com.example.day4;

import com.example.model.Transaction;
import com.example.transforms.JsonToTransactionTransform;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class StreamProcessingExample {


    public static void main(String[] args) {

        // FlinkPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
        // .withValidation()
        // .as(FlinkPipelineOptions.class);

        // options.setRunner(org.apache.beam.runners.flink.FlinkRunner.class);

        // options.setStreaming(true);
        // options.setCheckpointingInterval(5000L); // 5 seconds
        // options.setParallelism(1); // for simplicity

        StreamingOptions options = PipelineOptionsFactory.create().as(StreamingOptions.class);
        options.setRunner(DirectRunner.class); // Use DirectRunner for local execution
        options.setStreaming(true); // Set to true for streaming pipeline
        Pipeline pipeline = Pipeline.create(options);

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("group.id", "bean-pipeline-consumer-group-1");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.commit.interval.ms", "1000");


        pipeline.apply("ReadKafka", KafkaIO.<String, String>read()
                        .withBootstrapServers("localhost:9092")
                        .withTopic("transactions")
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withConsumerConfigUpdates(consumerProps)
                        .withoutMetadata())
                .apply("StringJSONToObject", ParDo.of(JsonToTransactionTransform.of()))
                // print the transaction object
                .apply("PrintTransaction", ParDo.of(new DoFn<Transaction, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Transaction transaction = c.element();
                        System.out.println("Received: " + transaction);
                    }
                }));

        pipeline.run().waitUntilFinish();
    }
}
