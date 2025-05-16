package com.example.chapter2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class TopKWordsDirect {

    public static void main(String[] args) {

        // Create pipeline
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        // Simulated input stream
        PCollection<String> lines = pipeline.apply("CreateInput", Create.of(
                "apple banana apple",
                "banana orange banana",
                "apple orange orange",
                "banana apple banana"
        ));

        // Step 2: Tokenize
        PCollection<String> words = lines.apply("SplitWords", FlatMapElements
                .into(TypeDescriptors.strings())
                .via(line -> java.util.Arrays.asList(line.split("\\s+"))));

        // Step 3: Apply fixed window of 15 seconds
        PCollection<String> windowedWords = words.apply("ApplyWindow", Window
                .into(FixedWindows.of(Duration.standardSeconds(15))));

        // Step 4: Count word frequencies
        PCollection<KV<String, Long>> wordCounts = windowedWords.apply("CountWords", Count.perElement());

        // Step 5: Top 3 frequent words
        PCollection<List<KV<String, Long>>> topWords = wordCounts.apply("TopKWords",
                Top.of(3, new SerializableComparator()).withoutDefaults());

        // Step 6: Flatten list result
        PCollection<KV<String, Long>> flattened = topWords.apply("FlattenList",
                FlatMapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                        .via(list -> list));


        // Step 7: Print to console
        flattened.apply("PrintResults", MapElements
                .into(TypeDescriptors.strings())
                .via(kv -> {
                    String out = kv.getKey() + " : " + kv.getValue();
                    System.out.println(out);
                    return out;
                }));

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }

    // Serializable comparator for sorting by value (descending)
    static class SerializableComparator implements Comparator<KV<String, Long>>, Serializable {
        public int compare(KV<String, Long> a, KV<String, Long> b) {
            return Long.compare(b.getValue(), a.getValue());
        }
    }
}
