package com.example.day6.transforms.window;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.apache.beam.sdk.values.KV;

public class WindowIntoSessionExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // ✅ Simulate events with timestamps (in event time)
        PCollection<String> events = pipeline.apply("CreateSessionEvents",
                Create.timestamped(
                        TimestampedValue.of("click", Instant.parse("2025-05-06T10:00:00Z")),
                        TimestampedValue.of("click", Instant.parse("2025-05-06T10:00:10Z")),
                        TimestampedValue.of("click", Instant.parse("2025-05-06T10:00:20Z")),
                        TimestampedValue.of("click", Instant.parse("2025-05-06T10:00:30Z")),
                        TimestampedValue.of("click", Instant.parse("2025-05-06T10:01:10Z")), // gap > 30s → new session
                        TimestampedValue.of("click", Instant.parse("2025-05-06T10:01:25Z"))
                ));

        // ✅ Apply Session windowing with 30-second inactivity gap
        PCollection<String> sessionized = events.apply("ApplySessionWindow",
                Window.into(Sessions.withGapDuration(Duration.standardSeconds(30))));

        // ✅ Count how many clicks occurred in each session
        PCollection<KV<String, Long>> sessionCounts = sessionized.apply("CountPerSession", Count.perElement());

        // ✅ Print the session window outputs
        sessionCounts.apply("PrintSessionCounts", MapElements.via(new SimpleFunction<KV<String, Long>, Void>() {
            @Override
            public Void apply(KV<String, Long> input) {
                System.out.println("Session Event: " + input.getKey() + " | Count: " + input.getValue());
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
