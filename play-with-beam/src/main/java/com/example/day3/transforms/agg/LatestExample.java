package com.example.transforms.agg;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Latest;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

public class LatestExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Sample KV data with events per user
        PCollection<KV<String, String>> events = pipeline.apply("CreateUserEvents",
                org.apache.beam.sdk.transforms.Create.of(
                        KV.of("user1", "login"),
                        KV.of("user1", "purchase"),
                        KV.of("user2", "view"),
                        KV.of("user2", "click")
                ));

        // Assign event-time timestamps based on value
        PCollection<KV<String, String>> timestamped = events.apply("AddTimestamps",
                WithTimestamps.of((KV<String, String> kv) -> {
                    switch (kv.getValue()) {
                        case "login": return Instant.parse("2023-01-01T10:00:00Z");
                        case "purchase": return Instant.parse("2023-01-01T10:10:00Z");
                        case "view": return Instant.parse("2023-01-01T09:55:00Z");
                        case "click": return Instant.parse("2023-01-01T10:05:00Z");
                        default: return Instant.now();
                    }
                }));

        // ✅ Latest.perKey()
        PCollection<KV<String, String>> latestPerUser = timestamped.apply("LatestPerKey", Latest.perKey());

        latestPerUser.apply("PrintLatestPerUser", MapElements.via(new SimpleFunction<KV<String, String>, Void>() {
            @Override
            public Void apply(KV<String, String> input) {
                System.out.println("User: " + input.getKey() + " → Latest Action: " + input.getValue());
                return null;
            }
        }));

        // Just values (globally)
        PCollection<String> values = timestamped.apply("ExtractValues", MapElements
                .into(org.apache.beam.sdk.values.TypeDescriptors.strings())
                .via(KV::getValue));

        // Reassign timestamps to values
        PCollection<String> valuesWithTimestamps = values.apply("AssignGlobalTimestamps",
                WithTimestamps.of((String event) -> {
                    switch (event) {
                        case "login": return Instant.parse("2023-01-01T10:00:00Z");
                        case "purchase": return Instant.parse("2023-01-01T10:10:00Z");
                        case "view": return Instant.parse("2023-01-01T09:55:00Z");
                        case "click": return Instant.parse("2023-01-01T10:05:00Z");
                        default: return Instant.now();
                    }
                }));

        // ✅ Latest.globally()
        PCollection<String> latestGlobal = valuesWithTimestamps.apply("LatestGlobally", Latest.globally());

        latestGlobal.apply("PrintLatestGlobally", MapElements.via(new SimpleFunction<String, Void>() {
            @Override
            public Void apply(String input) {
                System.out.println("Latest Event Globally: " + input);
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
