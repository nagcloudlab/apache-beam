package com.example.day3.transforms.agg;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

public class SumExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Example 1: Sum.perKey()
        List<KV<String, Integer>> data = Arrays.asList(
                KV.of("store1", 100),
                KV.of("store1", 200),
                KV.of("store2", 150),
                KV.of("store2", 50),
                KV.of("store3", 300)
        );

        PCollection<KV<String, Integer>> storeSales = pipeline
                .apply("CreatePerKeyData", org.apache.beam.sdk.transforms.Create.of(data));

        PCollection<KV<String, Integer>> salesSumPerStore = storeSales.apply("SumPerKey", Sum.integersPerKey());

        salesSumPerStore.apply("PrintPerKeySum", MapElements.via(new SimpleFunction<KV<String, Integer>, Void>() {
            @Override
            public Void apply(KV<String, Integer> input) {
                System.out.println(input.getKey() + ": " + input.getValue());
                return null;
            }
        }));

        // Example 2: Sum.globally()
        List<Integer> allSales = Arrays.asList(100, 200, 150, 50, 300);

        PCollection<Integer> globalSales = pipeline
                .apply("CreateGlobalData", org.apache.beam.sdk.transforms.Create.of(allSales));

        PCollection<Integer> totalSales = globalSales.apply("SumGlobally", Sum.integersGlobally());

        totalSales.apply("PrintGlobalSum", MapElements.via(new SimpleFunction<Integer, Void>() {
            @Override
            public Void apply(Integer input) {
                System.out.println("Total Sales: " + input);
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}