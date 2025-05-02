package com.example.first;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class PrintElementsTransform extends PTransform<PCollection<String>, PCollection<String>> {
    public static PrintElementsTransform of() {
        return new PrintElementsTransform();
    }
    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(c.element());
            }
        }));
    }
}
