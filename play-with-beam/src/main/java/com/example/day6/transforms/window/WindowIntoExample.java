package com.example.day6.transforms.window;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class WindowIntoExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Timestamped string events
        PCollection<String> events = pipeline.apply("CreateEvents",
                Create.timestamped(
                        TimestampedValue.of("click-A", Instant.parse("2025-05-06T10:00:05Z")),
                        TimestampedValue.of("click-B", Instant.parse("2025-05-06T10:00:25Z")),
                        TimestampedValue.of("click-C", Instant.parse("2025-05-06T10:01:10Z")),
                        TimestampedValue.of("click-D", Instant.parse("2025-05-06T10:01:45Z")),
                        TimestampedValue.of("click-E", Instant.parse("2025-05-06T10:02:05Z"))
                ));

        // Apply 1-minute Fixed Windowing
        PCollection<String> windowedEvents = events.apply("FixedWindow",
                Window.into(FixedWindows.of(Duration.standardMinutes(1))));

        // ✅ Count globally per window using Combine.globally() with Count.combineFn() and withoutDefaults()
        PCollection<Long> eventCounts = windowedEvents.apply("CountPerWindow",
                Combine.globally(Count.<String>combineFn()).withoutDefaults());

        // Print results
        eventCounts.apply("PrintCounts", MapElements
                        .into(TypeDescriptors.strings())
                        .via(count -> "Window count: " + count))
                .apply(MapElements.via(new SimpleFunction<String, Void>() {
                    @Override
                    public Void apply(String line) {
                        System.out.println(line);
                        return null;
                    }
                }));

        pipeline.run().waitUntilFinish();
    }
}
