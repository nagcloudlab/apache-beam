
package com.example.chapter2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Comparator;

public class Task2MaxWordLengthDirect {

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create(
                PipelineOptionsFactory.create()
        );

        // Step 1: Simulated input (no Kafka)
        PCollection<String> lines = pipeline
                .apply("CreateInput", Create.of(
                        "a", "bb", "ccc", "d", "elephant", "frog", "giraffe"
                ))
                .apply("AssignTimestamps", WithTimestamps.of((String word) -> {
                    long base = Instant.now().getMillis();
                    int index = word.length();
                    return new Instant(base + index * 10000L);
                }));


        // Step 2: Tokenize lines into words (already single words here)
        PCollection<String> words = lines;

        // Step 3: Global window with trigger after each element
        PCollection<String> windowed = words.apply("WindowIntoGlobal", Window.<String>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .withTimestampCombiner(TimestampCombiner.LATEST)
                .accumulatingFiredPanes()
        );

        // Step 4: Max word by length
        PCollection<String> longestWords = windowed.apply("ComputeMax",
                Max.globally((Serializable & Comparator<String>) (a, b) ->
                        Long.compare(a.length(), b.length()))
        );

        // Step 5: Add timestamps to output
        PCollection<KV<String, String>> withTimestamps = longestWords
                .apply("ReifyTimestamps", Reify.timestamps())
                .apply("ToKV", MapElements.into(TypeDescriptors.kvs(
                        TypeDescriptors.strings(),
                        TypeDescriptors.strings()
                )).via(tv -> KV.of(tv.getTimestamp().toString(), tv.getValue())));

        // Step 6: Print final output
        withTimestamps.apply("PrintResults", MapElements.into(TypeDescriptors.strings())
                .via(kv -> {
                    System.out.println(kv.getKey() + " => " + kv.getValue());
                    return kv.toString();
                }));

        pipeline.run().waitUntilFinish();
    }
}

