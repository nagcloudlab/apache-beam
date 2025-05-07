package com.example.day6.transforms.window;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.apache.beam.sdk.values.KV;

public class WindowIntoSlidingExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // ✅ Sample timestamped data
        PCollection<String> events = pipeline.apply("CreateEvents",
                Create.timestamped(
                        TimestampedValue.of("pageView", Instant.parse("2025-05-06T10:00:00Z")),
                        TimestampedValue.of("pageView", Instant.parse("2025-05-06T10:01:00Z")),
                        TimestampedValue.of("pageView", Instant.parse("2025-05-06T10:02:00Z")),
                        TimestampedValue.of("pageView", Instant.parse("2025-05-06T10:03:00Z")),
                        TimestampedValue.of("pageView", Instant.parse("2025-05-06T10:04:00Z"))
                ));

        // ✅ Apply Sliding Windows: 3-minute windows sliding every 1 minute
        PCollection<String> windowedEvents = events.apply("ApplySlidingWindow",
                Window.into(SlidingWindows.of(Duration.standardMinutes(3))
                        .every(Duration.standardMinutes(1))));

        // ✅ Count occurrences in each overlapping window
        PCollection<KV<String, Long>> counts = windowedEvents.apply("CountPerElement", Count.perElement());

        // ✅ Print windowed counts
        counts.apply("PrintCounts", MapElements.via(new SimpleFunction<KV<String, Long>, Void>() {
            @Override
            public Void apply(KV<String, Long> input) {
                System.out.println("Event: " + input.getKey() + " | Count: " + input.getValue());
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
