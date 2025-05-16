package com.example.chapter2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class Task2MaxWordLengthDirect2 {

    public static void main(String[] args) {

        // Create a Beam pipeline using default options
        Pipeline pipeline = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create()
        );

        // Simulate streaming data by emitting one element per second
        PCollection<String> lines = pipeline
                .apply("Generate dummy lines", GenerateSequence.from(0).to(8)
                        .withRate(1, Duration.standardSeconds(1))) // emit 1 per second
                .apply("Map numbers to words with timestamps", ParDo.of(new DoFn<Long, String>() {
                    @ProcessElement
                    public void processElement(@Element Long number, OutputReceiver<String> out, ProcessContext ctx) {
                        List<String> words = Arrays.asList("elephant", "a", "bbbbbbbbbbbbbbbbbbbbbbb", "ccc", "dddd", "eagle", "fish", "go");
                        String word = words.get(number.intValue());

                        // Simulate event-time by assigning a past timestamp based on index
                        Instant eventTime = Instant.now().minus(Duration.standardSeconds(10 * number));
                        ctx.outputWithTimestamp(word, eventTime);
                    }

                    @Override
                    public Duration getAllowedTimestampSkew() {
                        return Duration.standardDays(365); // or Duration.millis(Long.MAX_VALUE)
                    }
                }));

        // Extract the longest word seen so far using GlobalWindow
        lines
                // Apply Global Windowing with an early triggering strategy
                .apply("Global Window", Window.<String>into(new GlobalWindows())
                        .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(2))))
                        .accumulatingFiredPanes()
                        .withAllowedLateness(Duration.ZERO)
                        .withTimestampCombiner(TimestampCombiner.LATEST) // <- Important!
                )

                // Compute the word with the maximum length so far

                .apply("Longest word so far", Combine.globally(Max.of(new LongestWordComparator())))

                // Print the result with its output timestamp
                .apply("Print Output", ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(@Element String word, OutputReceiver<Void> out, ProcessContext ctx) {
                        System.out.println(ctx.timestamp() + " => " + word);
                    }
                }));

        pipeline.run().waitUntilFinish();
    }

    static class LongestWordComparator implements SerializableComparator<String> {
        @Override
        public int compare(String a, String b) {
            return Integer.compare(a.length(), b.length());
        }
    }
}
