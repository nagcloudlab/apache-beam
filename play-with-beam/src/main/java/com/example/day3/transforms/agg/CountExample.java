package com.example.transforms.agg;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

public class CountExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Sample data
        PCollection<String> words = pipeline.apply("CreateWords",
                org.apache.beam.sdk.transforms.Create.of(
                        "apple", "banana", "apple", "orange", "banana", "banana"
                ));

        // ✅ Count.perElement() — Word Frequency
        PCollection<KV<String, Long>> wordCounts = words.apply("CountWords", Count.perElement());

        wordCounts.apply("PrintWordCounts", MapElements.via(new SimpleFunction<KV<String, Long>, Void>() {
            @Override
            public Void apply(KV<String, Long> input) {
                System.out.println("Word: " + input.getKey() + " → Count: " + input.getValue());
                return null;
            }
        }));

        // ✅ Count.perKey() — Simulate key-value pairs
        PCollection<KV<String, String>> events = pipeline.apply("CreateKeyedEvents",
                org.apache.beam.sdk.transforms.Create.of(
                        KV.of("user1", "click"),
                        KV.of("user1", "scroll"),
                        KV.of("user2", "click"),
                        KV.of("user1", "click"),
                        KV.of("user2", "view")
                ));

        PCollection<KV<String, Long>> eventsPerUser = events.apply("CountEventsPerUser", Count.perKey());

        eventsPerUser.apply("PrintUserEventCounts", MapElements.via(new SimpleFunction<KV<String, Long>, Void>() {
            @Override
            public Void apply(KV<String, Long> input) {
                System.out.println("User: " + input.getKey() + " → Events: " + input.getValue());
                return null;
            }
        }));

        // ✅ Count.globally() — Total number of elements
        PCollection<Long> totalCount = words.apply("CountAllWords", Count.globally());

        totalCount.apply("PrintTotalCount", MapElements.via(new SimpleFunction<Long, Void>() {
            @Override
            public Void apply(Long input) {
                System.out.println("Total Words: " + input);
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
