package com.example.transforms.agg;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

public class CombineCustomExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Sample KV data: messages per user
        PCollection<KV<String, String>> userMessages = pipeline.apply("CreateMessages",
                org.apache.beam.sdk.transforms.Create.of(
                        KV.of("alice", "Hi"),
                        KV.of("alice", "Hello"),
                        KV.of("bob", "Good morning"),
                        KV.of("bob", "Bye"),
                        KV.of("alice", "Beam")
                ));

        // ✅ Combine.perKey() — total character count per user
        PCollection<KV<String, Integer>> charCountPerUser = userMessages.apply(
                "CharCountPerUser",
                Combine.perKey(new Combine.CombineFn<String, int[], Integer>() {
                    public int[] createAccumulator() {
                        return new int[]{0};
                    }

                    public int[] addInput(int[] acc, String input) {
                        acc[0] += input.length();
                        return acc;
                    }

                    public int[] mergeAccumulators(Iterable<int[]> accs) {
                        int sum = 0;
                        for (int[] a : accs) sum += a[0];
                        return new int[]{sum};
                    }

                    public Integer extractOutput(int[] acc) {
                        return acc[0];
                    }
                })
        );

        charCountPerUser.apply("PrintCharCounts", MapElements.via(new SimpleFunction<KV<String, Integer>, Void>() {
            @Override
            public Void apply(KV<String, Integer> input) {
                System.out.println("User: " + input.getKey() + " → Total Characters: " + input.getValue());
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}

