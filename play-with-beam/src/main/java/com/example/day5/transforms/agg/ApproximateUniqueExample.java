package com.example.day5.transforms.agg;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.ApproximateUnique;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

public class ApproximateUniqueExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // ✅ ApproximateUnique.perKey() — estimate unique actions per user
        PCollection<KV<String, String>> userActions = pipeline.apply("CreateUserActions",
                org.apache.beam.sdk.transforms.Create.of(
                        KV.of("user1", "click"),
                        KV.of("user1", "click"),
                        KV.of("user1", "view"),
                        KV.of("user2", "click"),
                        KV.of("user2", "view"),
                        KV.of("user2", "scroll"),
                        KV.of("user2", "scroll")
                ));

        PCollection<KV<String, Long>> approxUniquePerUser = userActions
                .apply("EstimateUniqueActions", ApproximateUnique.perKey(128));

        approxUniquePerUser.apply("PrintApproxUniquePerUser", MapElements.via(new SimpleFunction<KV<String, Long>, Void>() {
            @Override
            public Void apply(KV<String, Long> input) {
                System.out.println("User: " + input.getKey() + " → Estimated Unique Actions: " + input.getValue());
                return null;
            }
        }));

        // ✅ ApproximateUnique.globally() — estimate total unique events
        PCollection<String> eventStream = pipeline.apply("CreateGlobalEvents",
                org.apache.beam.sdk.transforms.Create.of(
                        "click", "scroll", "view", "click", "view", "hover", "scroll", "click", "submit"
                ));

        PCollection<Long> approxUniqueGlobal = eventStream
                .apply("EstimateGlobalUniques", ApproximateUnique.globally(128));

        approxUniqueGlobal.apply("PrintApproxUniqueGlobal", MapElements.via(new SimpleFunction<Long, Void>() {
            @Override
            public Void apply(Long input) {
                System.out.println("Estimated Unique Events Globally: " + input);
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
