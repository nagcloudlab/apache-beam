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

public class WindowWithLateness {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());

        TestStream<String> stream = TestStream.create(StringUtf8Coder.of())
                .addElements(
                        TimestampedValue.of("event", Instant.parse("2025-05-06T10:00:10Z")),
                        TimestampedValue.of("event", Instant.parse("2025-05-06T10:00:20Z"))
                )
                .advanceWatermarkTo(Instant.parse("2025-05-06T10:01:00Z"))
                .addElements(TimestampedValue.of("event", Instant.parse("2025-05-06T10:00:40Z"))) // Late!
                .advanceWatermarkToInfinity();

        PCollection<String> input = pipeline.apply("SimulatedEvents", stream);

        PCollection<String> windowed = input.apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                .withAllowedLateness(Duration.standardMinutes(1)) // ✅ Late data allowed
                .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                .accumulatingFiredPanes()
        );

        PCollection<KV<String, Long>> counted = windowed.apply(Count.perElement());

        counted.apply("Print", MapElements.via(new SimpleFunction<KV<String, Long>, Void>() {
            @Override
            public Void apply(KV<String, Long> input) {
                System.out.println("[ALLOW LATE] Element: " + input.getKey() + " | Count: " + input.getValue());
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
