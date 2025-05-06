package com.example.day5.transforms.element_wise;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class FlatMapElementsExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> sentences = pipeline.apply("CreateSentences",
                org.apache.beam.sdk.transforms.Create.of(
                        "Apache Beam is powerful",
                        "Java SDK is cool",
                        "Transform and scale"
                ));

        // ✅ Split sentences into words
        PCollection<String> words = sentences.apply("SplitWords",
                FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via((String sentence) -> Arrays.asList(sentence.split(" ")))
        );

        words.apply("PrintWords", MapElements.via(new SimpleFunction<String, Void>() {
            @Override
            public Void apply(String input) {
                System.out.println("Word: " + input);
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
