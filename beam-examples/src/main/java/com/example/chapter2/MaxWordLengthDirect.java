package com.example.chapter2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class MaxWordLengthDirect {

    public static void main(String[] args) {

        // Step 1: Create pipeline
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        // Step 2: Simulated batch input
        PCollection<String> lines = pipeline.apply("ReadLines", Create.of(
                "hello world",
                "apache beam rocks",
                "stream and batch nnnnnnnnnnnnnnnnnnnnn"
        ));

        // Step 3: Tokenize words
        PCollection<String> words = lines.apply("SplitWords", FlatMapElements
                .into(TypeDescriptors.strings())
                .via(line -> java.util.Arrays.asList(line.split("\\s+"))));

        // Step 4: Convert each word to its length
        PCollection<Integer> wordLengths = words.apply("MapToLength", MapElements
                .into(TypeDescriptors.integers())
                .via(String::length));

        // Step 5: Compute maximum length
        PCollection<Integer> maxLength = wordLengths.apply("FindMax", Combine.globally(Max.ofIntegers()));

        // Step 6: Print the result
        maxLength.apply("PrintMax", MapElements
                .into(TypeDescriptors.strings())
                .via(len -> {
                    String out = "Max word length: " + len;
                    System.out.println(out);
                    return out;
                }));

        // Step 7: Run
        pipeline.run().waitUntilFinish();
    }
}
