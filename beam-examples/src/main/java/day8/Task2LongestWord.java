package day8;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import java.io.Serializable;
import java.util.Comparator;

public class Task2LongestWord {

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).create()
        );

        pipeline
                .apply("ReadLines", Create.of(
                        "hi apple",
                        "banana",
                        "zebra elephant",
                        "monkey",
                        "hippopotamus"
                ))
                .apply("Tokenize", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via(line -> java.util.Arrays.asList(line.split("\\s+"))))
                .apply("GlobalWindow", Window.<String>into(new GlobalWindows())
                        .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                        .accumulatingFiredPanes()
                        .withAllowedLateness(Duration.ZERO)
                )
                .apply("FindLongest", Max.globally(new LongestWordComparator()))
                .apply("Print", MapElements.into(TypeDescriptors.strings())
                        .via(word -> {
                            System.out.println("Longest word so far: " + word);
                            return word;
                        }));

        pipeline.run().waitUntilFinish();
    }

    static class LongestWordComparator implements Comparator<String>, Serializable {
        public int compare(String a, String b) {
            return Integer.compare(a.length(), b.length());
        }
    }
}
