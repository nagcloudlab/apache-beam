package com.example.day3.transforms.agg;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

public class MaxMinExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Sample data for global min/max
        PCollection<Integer> scores = pipeline.apply("CreateScores",
                org.apache.beam.sdk.transforms.Create.of(88, 92, 76, 99, 85));

        // ✅ Max.globally()
        PCollection<Integer> maxScore = scores.apply("MaxGlobally", Max.integersGlobally());
        maxScore.apply("PrintMaxGlobally", MapElements.via(new SimpleFunction<Integer, Void>() {
            @Override
            public Void apply(Integer input) {
                System.out.println("Highest Score: " + input);
                return null;
            }
        }));

        // ✅ Min.globally()
        PCollection<Integer> minScore = scores.apply("MinGlobally", Min.integersGlobally());
        minScore.apply("PrintMinGlobally", MapElements.via(new SimpleFunction<Integer, Void>() {
            @Override
            public Void apply(Integer input) {
                System.out.println("Lowest Score: " + input);
                return null;
            }
        }));

        // Sample keyed data for perKey min/max
        PCollection<KV<String, Integer>> userScores = pipeline.apply("CreateUserScores",
                org.apache.beam.sdk.transforms.Create.of(
                        KV.of("Alice", 88),
                        KV.of("Alice", 92),
                        KV.of("Bob", 76),
                        KV.of("Bob", 99),
                        KV.of("Alice", 85)
                ));

        // ✅ Max.perKey()
        PCollection<KV<String, Integer>> maxPerUser = userScores.apply("MaxPerUser", Max.integersPerKey());
        maxPerUser.apply("PrintMaxPerUser", MapElements.via(new SimpleFunction<KV<String, Integer>, Void>() {
            @Override
            public Void apply(KV<String, Integer> input) {
                System.out.println("User: " + input.getKey() + " → Max Score: " + input.getValue());
                return null;
            }
        }));

        // ✅ Min.perKey()
        PCollection<KV<String, Integer>> minPerUser = userScores.apply("MinPerUser", Min.integersPerKey());
        minPerUser.apply("PrintMinPerUser", MapElements.via(new SimpleFunction<KV<String, Integer>, Void>() {
            @Override
            public Void apply(KV<String, Integer> input) {
                System.out.println("User: " + input.getKey() + " → Min Score: " + input.getValue());
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}

