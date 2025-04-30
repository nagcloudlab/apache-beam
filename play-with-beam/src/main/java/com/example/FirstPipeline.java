package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;

public class FirstPipeline {
    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

        pipeline
                .apply("ReadLines", TextIO.read().from("/Users/nag/apache-beam/play-with-beam/input.txt"))
                .apply("ConvertToUpperCase",MapElements.into(TypeDescriptors.strings()).via(line->line.toUpperCase()))
                .apply("WriteLines", TextIO.write().to("output").withoutSharding().withSuffix(".txt"));
     
        pipeline.run().waitUntilFinish();        
    }
}