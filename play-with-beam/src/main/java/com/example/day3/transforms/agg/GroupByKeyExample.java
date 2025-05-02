package com.example.day3.transforms.agg;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

public class GroupByKeyExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Sample KV data: messages per topic
        PCollection<KV<String, String>> messages = pipeline.apply("CreateMessages",
                org.apache.beam.sdk.transforms.Create.of(
                        KV.of("topic1", "msg1"),
                        KV.of("topic1", "msg2"),
                        KV.of("topic2", "msg3"),
                        KV.of("topic1", "msg4"),
                        KV.of("topic2", "msg5")
                ));

        // ✅ GroupByKey
        PCollection<KV<String, Iterable<String>>> grouped = messages.apply("GroupByTopic", GroupByKey.create());

        // Print grouped results
        grouped.apply("PrintGrouped", MapElements.via(new SimpleFunction<KV<String, Iterable<String>>, Void>() {
            @Override
            public Void apply(KV<String, Iterable<String>> input) {
                System.out.println("Topic: " + input.getKey() + " → Messages: " + input.getValue());
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
