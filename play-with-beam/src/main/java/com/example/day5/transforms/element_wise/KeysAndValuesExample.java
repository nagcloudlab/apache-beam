package com.example.day5.transforms.element_wise;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

public class KeysAndValuesExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Sample KV data: product → price
        PCollection<KV<String, Integer>> products = pipeline.apply("CreateProducts",
                org.apache.beam.sdk.transforms.Create.of(
                        KV.of("Apple", 120),
                        KV.of("Banana", 80),
                        KV.of("Cherry", 150)
                ));

        // ✅ Extract just the keys (product names)
        PCollection<String> productNames = products.apply("ExtractKeys", Keys.create());

        productNames.apply("PrintProductNames", MapElements.via(new SimpleFunction<String, Void>() {
            @Override
            public Void apply(String input) {
                System.out.println("Product: " + input);
                return null;
            }
        }));

        // ✅ Extract just the values (prices)
        PCollection<Integer> productPrices = products.apply("ExtractValues", Values.create());

        productPrices.apply("PrintPrices", MapElements.via(new SimpleFunction<Integer, Void>() {
            @Override
            public Void apply(Integer input) {
                System.out.println("Price: ₹" + input);
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
