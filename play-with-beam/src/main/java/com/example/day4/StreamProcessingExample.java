package com.example.day4;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StreamProcessingExample {

    public static void main(String[] args) {

        FlinkPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(FlinkPipelineOptions.class);

        options.setRunner(org.apache.beam.runners.flink.FlinkRunner.class);

        options.setStreaming(true);
        options.setCheckpointingInterval(5000L); // 5 seconds
        options.setParallelism(1); // for simplicity

        Pipeline pipeline = Pipeline.create(options);

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("group.id", "beam-flink-consumer-group");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("enable.auto.commit", "true"); // optional, Beam still manages commits


        pipeline.apply("ReadKafka", KafkaIO.<String, String>read()
                        .withBootstrapServers("localhost:9092")
                        .withTopic("transactions")
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .updateConsumerProperties(consumerProps)
                        .withoutMetadata())
                .apply("PrintValues", MapElements.via(new SimpleFunction<KV<String, String>, Void>() {
                    @Override
                    public Void apply(KV<String, String> kv) {
                        System.out.println("Received: " + kv.getValue());
                        return null;
                    }
                }));
        pipeline.run().waitUntilFinish();
    }
}
