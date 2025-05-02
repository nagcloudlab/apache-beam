package com.example.transforms.agg;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

public class MeanExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Example 1: Mean.perKey()
        List<KV<String, Double>> ratings = Arrays.asList(
                KV.of("product1", 4.5),
                KV.of("product1", 3.5),
                KV.of("product2", 5.0),
                KV.of("product2", 4.0),
                KV.of("product3", 2.0)
        );

        PCollection<KV<String, Double>> productRatings = pipeline
                .apply("CreateRatingData", org.apache.beam.sdk.transforms.Create.of(ratings));

        PCollection<KV<String, Double>> avgRatings = productRatings.apply("AveragePerProduct", Mean.perKey());

        avgRatings.apply("PrintAveragePerProduct", MapElements.via(new SimpleFunction<KV<String, Double>, Void>() {
            @Override
            public Void apply(KV<String, Double> input) {
                System.out.println(input.getKey() + ": Avg Rating = " + input.getValue());
                return null;
            }
        }));

        // Example 2: Mean.globally()
        List<Double> latencies = Arrays.asList(100.0, 200.0, 150.0, 50.0, 300.0);

        PCollection<Double> latencyData = pipeline
                .apply("CreateLatencyData", org.apache.beam.sdk.transforms.Create.of(latencies));

        PCollection<Double> avgLatency = latencyData.apply("AverageGlobally", Mean.globally());

        avgLatency.apply("PrintGlobalAverage", MapElements.via(new SimpleFunction<Double, Void>() {
            @Override
            public Void apply(Double input) {
                System.out.println("Average Latency: " + input);
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}