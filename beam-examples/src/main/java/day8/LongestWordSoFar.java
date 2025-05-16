package com.example.chapter2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Comparator;

public class LongestWordSoFar {

    public static void main(String[] args) {

        // Step 1: Create pipeline
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        // Step 2: Simulated input stream
        PCollection<String> lines = pipeline.apply("CreateInput", Create.of(
                        "apple banana",
                        "watermelon orange",
                        "pineapple",
                        "grapefruit",
                        "kiwi"
                ))
                // Step 2.1: Assign artificial timestamps to simulate streaming
                .apply("AssignTimestamps", WithTimestamps.of((String line) -> Instant.now()));

        // Step 3: Split lines into words
        PCollection<String> words = lines.apply("SplitWords", FlatMapElements
                .into(TypeDescriptors.strings())
                .via(line -> java.util.Arrays.asList(line.split("\\s+"))));

        // Step 4: Apply Global Window with trigger on every element
        PCollection<String> globallyWindowed = words.apply("GlobalWindow", Window.<String>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .accumulatingFiredPanes()
                .withAllowedLateness(Duration.ZERO));

        // Step 5: Compute longest word seen so far
        PCollection<String> longestWord = globallyWindowed.apply("LongestWord",
                Combine.globally(Max.of(new LongestWordComparator())).withoutDefaults());

        // Step 6: Print each new longest word to console
        longestWord.apply("PrintResults", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext ctx) {
                System.out.println("Longest so far: " + ctx.element());
            }
        }));

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }

    // Custom comparator to find longest word
    static class LongestWordComparator implements Comparator<String>, Serializable {
        public int compare(String a, String b) {
            return Integer.compare(a.length(), b.length());
        }
    }
}
