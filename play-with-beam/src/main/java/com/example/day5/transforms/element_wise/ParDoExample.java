package com.example.day5.transforms.element_wise;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

class TaggedOutput extends DoFn<Integer, String> {
    public static final String EVEN = "Even";
    public static final String ODD = "Odd";

    @ProcessElement
    public void processElement(@Element Integer number, OutputReceiver<String> out) {
        if (number % 2 == 0) {
            out.output(EVEN + ": " + number);
        } else {
            out.output(ODD + ": " + number);
        }
    }

    public static TaggedOutput of() {
        return new TaggedOutput();
    }

}

public class ParDoExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<Integer> numbers = pipeline.apply(org.apache.beam.sdk.transforms.Create.of(
                1, 2, 3, 4, 5
        ));

        PCollection<String> tagged = numbers.apply(ParDo.of(TaggedOutput.of()));

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
