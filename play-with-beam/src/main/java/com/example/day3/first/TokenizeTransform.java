package com.example.first;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class TokenizeForm extends PTransform<PCollection<String>, PCollection<String>> {
    // factory method, to create a new instance of the class
    public static TokenizeForm of() {
        return new TokenizeForm();
    }

    // transform method, to transform the input PCollection
    @Override
    public PCollection<String> expand(PCollection<String> input) {
        //return input.apply(FlatMapElements.into(TypeDescriptors.strings()).via(line->Arrays.asList(line.split("[^\\p{L}]"))));
        return input.apply(
                ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String line = c.element();
                        String[] words = line.split("[^\\p{L}]");
                        for (String word : words) {
                            c.output(word);
                        }
                    }
                }));
    }
}
