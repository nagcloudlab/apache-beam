package com.example.transforms.element_wise;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

public class WithKeysDynamicExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        List<String> carnivores = Arrays.asList("lion", "tiger", "leopard");

        PCollection<String> animals = pipeline.apply(org.apache.beam.sdk.transforms.Create.of(
                "lion", "tiger", "zebra", "deer", "elephant"
        ));

        PCollection<KV<String, String>> keyedAnimals = animals.apply(
                WithKeys.of((String animal) -> carnivores.contains(animal) ? "carnivore" : "herbivore")
                        .withKeyType(TypeDescriptors.strings())
        );

        keyedAnimals.apply("PrintKeyedAnimals", MapElements.via(new SimpleFunction<KV<String, String>, Void>() {
            @Override
            public Void apply(KV<String, String> input) {
                System.out.println("Key: " + input.getKey() + ", Value: " + input.getValue());
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
