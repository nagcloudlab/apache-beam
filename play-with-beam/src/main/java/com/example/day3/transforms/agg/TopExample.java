package com.example.transforms.agg;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.SerializableComparator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

// Custom comparator that is serializable
class IntegerDescendingComparator implements SerializableComparator<Integer> {
    @Override
    public int compare(Integer a, Integer b) {
        return b.compareTo(a); // descending order
    }
}

public class TopExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Sample KV data: student scores
        List<KV<String, Integer>> scores = Arrays.asList(
                KV.of("Alice", 80),
                KV.of("Bob", 95),
                KV.of("Alice", 92),
                KV.of("Bob", 87),
                KV.of("Alice", 76),
                KV.of("Bob", 91)
        );

        PCollection<KV<String, Integer>> scoreData = pipeline
                .apply("CreateScoreData", org.apache.beam.sdk.transforms.Create.of(scores));

        // ✅ Top 2 scores per student (perKey)
        PCollection<KV<String, List<Integer>>> top2PerStudent = scoreData.apply(
                "Top2PerKey",
                Top.perKey(2, new IntegerDescendingComparator())
        );

        top2PerStudent.apply("PrintTopPerKey", MapElements.via(new SimpleFunction<KV<String, List<Integer>>, Void>() {
            @Override
            public Void apply(KV<String, List<Integer>> input) {
                System.out.println("Student: " + input.getKey() + " → Top Scores: " + input.getValue());
                return null;
            }
        }));

        // Sample prices for Top.of
        List<Integer> prices = Arrays.asList(120, 450, 320, 180, 900, 610);

        PCollection<Integer> priceData = pipeline
                .apply("CreatePrices", org.apache.beam.sdk.transforms.Create.of(prices));

        // ✅ Top 3 prices globally (Top.of)
        PCollection<List<Integer>> top3Prices = priceData.apply("Top3Prices",
                Top.of(3, new IntegerDescendingComparator()));

        top3Prices.apply("PrintTopGlobal", MapElements.via(new SimpleFunction<List<Integer>, Void>() {
            @Override
            public Void apply(List<Integer> input) {
                System.out.println("Top 3 Prices: " + input);
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
