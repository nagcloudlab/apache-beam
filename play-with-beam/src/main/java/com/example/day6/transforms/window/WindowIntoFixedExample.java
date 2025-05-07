package com.example.day6.transforms.window;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class WindowIntoFixedExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // ✅ Create timestamped data to simulate event time
        PCollection<String> events = pipeline.apply("CreateEvents",
                Create.timestamped(
                        TimestampedValue.of("userA", Instant.parse("2025-05-06T10:00:05Z")),
                        TimestampedValue.of("userA", Instant.parse("2025-05-06T10:00:25Z")),
                        TimestampedValue.of("userB", Instant.parse("2025-05-06T10:01:10Z")),
                        TimestampedValue.of("userB", Instant.parse("2025-05-06T10:01:45Z")),
                        TimestampedValue.of("userC", Instant.parse("2025-05-06T10:02:05Z")),
                        TimestampedValue.of("userC", Instant.parse("2025-05-06T10:02:15Z")),
                        TimestampedValue.of("userC", Instant.parse("2025-05-06T10:02:45Z"))
                ));

        // ✅ Apply Fixed Windowing of 1 minute
        PCollection<String> windowedEvents = events.apply("ApplyFixedWindow",
                Window.into(FixedWindows.of(Duration.standardMinutes(1))));

        // ✅ Count how many times each element appears in each window
        PCollection<org.apache.beam.sdk.values.KV<String, Long>> counted = windowedEvents
                .apply("CountPerElement", Count.perElement());

        // ✅ Print the results
        counted.apply("PrintCounts", MapElements.via(new SimpleFunction<org.apache.beam.sdk.values.KV<String, Long>, Void>() {
            @Override
            public Void apply(org.apache.beam.sdk.values.KV<String, Long> input) {
                System.out.println("Element: " + input.getKey() + " | Count: " + input.getValue());
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
