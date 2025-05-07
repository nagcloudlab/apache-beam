package com.example.day6.transforms.window;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class WindowWithLateness {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> onTime = pipeline.apply("OnTimeEvents",
                Create.timestamped(
                        TimestampedValue.of("event", Instant.parse("2025-05-06T10:00:10Z")),
                        TimestampedValue.of("event", Instant.parse("2025-05-06T10:00:20Z"))
                ));

        PCollection<String> late = pipeline
                .apply("LateEvent", Create.timestamped(
                        TimestampedValue.of("event", Instant.parse("2025-05-06T10:01:30Z"))
                ))
                .apply("DelayLateEvent", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext ctx) throws InterruptedException {
                        Thread.sleep(2000); // Simulate late arrival
                        ctx.output(ctx.element());
                    }
                }));

        PCollection<String> merged = PCollectionList.of(onTime).and(late)
                .apply(Flatten.pCollections());

        PCollection<KV<String, Long>> result = merged
                .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                        .withAllowedLateness(Duration.standardMinutes(1))
                        .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                        .accumulatingFiredPanes()
                )
                .apply(Count.perElement());

        result.apply("Print", MapElements.via(new SimpleFunction<KV<String, Long>, Void>() {
            @Override
            public Void apply(KV<String, Long> input) {
                System.out.println("[WITH LATENESS] Element: " + input.getKey() + " | Count: " + input.getValue());
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
