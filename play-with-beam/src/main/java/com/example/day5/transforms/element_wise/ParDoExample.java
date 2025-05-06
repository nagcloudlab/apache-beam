package com.example.day3.transforms.element_wise;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class ParDoExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<Integer> numbers = pipeline.apply(org.apache.beam.sdk.transforms.Create.of(
                1, 2, 3, 4, 5
        ));

        PCollection<String> tagged = numbers.apply(
                ParDo.of(new DoFn<Integer, String>() {
                    @ProcessElement
                    public void processElement(@Element Integer number, OutputReceiver<String> out) {
                        if (number % 2 == 0) {
                            out.output("Even: " + number);
                        } else {
                            out.output("Odd: " + number);
                        }
                    }
                })
        );

        tagged.apply("PrintTagged", MapElements.via(new SimpleFunction<String, Void>() {
            @Override
            public Void apply(String input) {
                System.out.println(input);
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
