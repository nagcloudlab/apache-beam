package com.example.day3.transforms.element_wise;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class MapElementsExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> names = pipeline.apply("CreateNames",
                org.apache.beam.sdk.transforms.Create.of(
                        "alice", "bob", "charlie"
                ));

        // ✅ Transform: lowercase to uppercase
        PCollection<String> upperNames = names.apply("ToUpperCase",
                MapElements
                        .into(TypeDescriptors.strings())
                        .via((String name) -> name.toUpperCase())
        );

        upperNames.apply("PrintUpperNames", MapElements.via(new SimpleFunction<String, Void>() {
            @Override
            public Void apply(String input) {
                System.out.println("Upper: " + input);
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
