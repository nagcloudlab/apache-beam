package com.example.day5.transforms.element_wise;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

public class FilterExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<Integer> numbers = pipeline.apply("CreateNumbers",
                org.apache.beam.sdk.transforms.Create.of(1, 2, 3, 4, 5, 6, 7));

        // ✅ Keep only even numbers
        PCollection<Integer> evens = numbers.apply("FilterEven",
                Filter.by((Integer x) -> x % 2 == 0));

        evens.apply("PrintEvens", MapElements.via(new SimpleFunction<Integer, Void>() {
            @Override
            public Void apply(Integer input) {
                System.out.println("Even: " + input);
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
