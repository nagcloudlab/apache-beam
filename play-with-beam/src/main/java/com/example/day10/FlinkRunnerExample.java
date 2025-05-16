package com.example.day10;

import java.util.Arrays;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public class FlinkRunnerExample {
        public static void main(String[] args) {
                // Create a Flink pipeline
                FlinkPipelineOptions options = PipelineOptionsFactory.create().as(FlinkPipelineOptions.class);
                options.setRunner(FlinkRunner.class);
                options.setStreaming(false);
                options.setCheckpointingInterval(5000L); // 5 seconds
                options.setParallelism(1); // for simplicity
                options.setFlinkMaster("localhost:8081"); // or jobmanager:8081 in Docker/K8s


                Pipeline pipeline = Pipeline.create(options);

                pipeline.apply("ReadLines", TextIO.read().from("/Users/nag/apache-beam/input.txt"))
                                .apply("SplitWords", FlatMapElements.into(TypeDescriptors.strings())
                                                .via((String line) -> Arrays.asList(line.split("\\W+"))))
                                .apply("PairWithOne",
                                                MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                                                                TypeDescriptors.integers()))
                                                                .via((String word) -> KV.of(word, 1)))
                                .apply("CountWords", Sum.integersPerKey())
                                .apply("FormatAsText", MapElements.into(TypeDescriptors.strings())
                                                .via((KV<String, Integer> wordCount) -> wordCount.getKey() + ": "
                                                                + wordCount.getValue()))
                                .apply("WriteCounts", TextIO.write().to("output.txt"));

                // Run the pipeline
                pipeline.run().waitUntilFinish();

        }
}
