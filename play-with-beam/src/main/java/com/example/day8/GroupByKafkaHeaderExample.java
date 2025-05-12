package com.example.day8;

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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

import java.util.HashMap;
import java.util.Map;

public class GroupByKafkaHeaderExample {

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create()
        );


        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("group.id", "bean-pipeline-consumer-group-11");
        consumerProps.put("auto.offset.reset", "earliest");

        // Step 1: Read KafkaRecord (retain metadata like headers)
        var kafkaRecords = pipeline.apply("ReadFromKafka", KafkaIO.<String, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("transactions")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                //.withTimestampPolicyFactory(TimestampPolicyFactory.withLogAppendTime())
        );

        // Step 2: Extract header value ("region") as grouping key
        var regionAndValue = kafkaRecords.apply("ExtractRegionHeader", MapElements
                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via((KafkaRecord<String, String> record) -> {
                    String value = record.getKV().getValue();
                    String region = "UNKNOWN";

                    if (record.getHeaders() != null) {
                        Header header = record.getHeaders().lastHeader("region");
                        if (header != null) {
                            region = new String(header.value()); // decode byte[] to String
                        }
                    }

                    return KV.of(region, value);
                }));

        // Step 3: Apply 15-second window
        var windowed = regionAndValue.apply("Window15s", Window.into(FixedWindows.of(Duration.standardSeconds(15))));

        // Step 4: Group by region and count
        var grouped = windowed
                .apply("GroupByRegion", GroupByKey.create())
                .apply("CountPerRegion", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Iterable<String>> kv) -> {
                            long count = 0;
                            for (String val : kv.getValue()) {
                                count++;
                            }
                            return "Region: " + kv.getKey() + " | Count: " + count;
                        }));

        // Step 5: Print results
        grouped.apply("Print", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(c.element());
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
