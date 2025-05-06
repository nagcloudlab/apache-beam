package com.example.day5.transforms.element_wise;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class WithKeysExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> words = pipeline.apply("CreateWords",
                org.apache.beam.sdk.transforms.Create.of("apple", "banana", "apricot", "blueberry", "cherry"));

        // ✅ Add key: first character of each word (explicit key type)
        PCollection<KV<String, String>> keyedWords = words.apply("KeyByFirstLetter",
                WithKeys.of((String word) -> word.substring(0, 1))
                        .withKeyType(TypeDescriptors.strings()));

        keyedWords.apply("PrintKeyedWords", MapElements.via(new SimpleFunction<KV<String, String>, Void>() {
            @Override
            public Void apply(KV
                                      <String, String> input) {
                System.out.println("Key: " + input.getKey() + " → Word: " + input.getValue());
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
