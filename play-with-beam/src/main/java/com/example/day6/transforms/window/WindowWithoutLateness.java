package com.example.beam.windowing;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.Duration;
import org.joda.time.Instant;


// drop late data

public class WindowWithoutLateness {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());

        // Simulate stream with 2 on-time + 1 late element
        TestStream<String> stream = TestStream.create(StringUtf8Coder.of())
                .addElements(
                        TimestampedValue.of("event", Instant.parse("2025-05-06T10:00:10Z")),
                        TimestampedValue.of("event", Instant.parse("2025-05-06T10:00:20Z"))
                )
                .advanceWatermarkTo(Instant.parse("2025-05-06T10:01:00Z")) // end of window
                .addElements(TimestampedValue.of("event", Instant.parse("2025-05-06T10:00:40Z"))) // LATE!
                .advanceWatermarkToInfinity();

        PCollection<String> input = pipeline.apply("SimulatedEvents", stream);

        PCollection<String> windowed = input.apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                .withAllowedLateness(Duration.ZERO) // ❌ No late data accepted
                .triggering(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
        );

        PCollection<KV<String, Long>> counted = windowed.apply(Count.perElement());

        counted.apply("Print", MapElements.via(new SimpleFunction<KV<String, Long>, Void>() {
            @Override
            public Void apply(KV<String, Long> input) {
                System.out.println("[DROP LATE] Element: " + input.getKey() + " | Count: " + input.getValue());
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
