package com.example.transforms.element_wise;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.*;

import java.util.Arrays;

public class ParDoWithOutputTagsExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Input data: numbers
        PCollection<Integer> numbers = pipeline.apply("CreateNumbers",
                org.apache.beam.sdk.transforms.Create.of(1, 2, 3, 4, 5, 6));

        // Define output tags
        final TupleTag<Integer> evenTag = new TupleTag<Integer>(){};
        final TupleTag<Integer> oddTag = new TupleTag<Integer>(){};

        // Define ParDo with output tags
        PCollectionTuple results = numbers.apply("SplitEvenOdd", ParDo.of(new DoFn<Integer, Integer>() {
            @ProcessElement
            public void processElement(@Element Integer number, MultiOutputReceiver out) {
                if (number % 2 == 0) {
                    out.get(evenTag).output(number);
                } else {
                    out.get(oddTag).output(number);
                }
            }
        }).withOutputTags(evenTag, TupleTagList.of(oddTag)));

        // Get the output collections
        PCollection<Integer> evens = results.get(evenTag);
        PCollection<Integer> odds = results.get(oddTag);

        // Print evens
        evens.apply("PrintEvens", MapElements.via(new SimpleFunction<Integer, Void>() {
            @Override
            public Void apply(Integer input) {
                System.out.println("Even: " + input);
                return null;
            }
        }));

        // Print odds
        odds.apply("PrintOdds", MapElements.via(new SimpleFunction<Integer, Void>() {
            @Override
            public Void apply(Integer input) {
                System.out.println("Odd: " + input);
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
