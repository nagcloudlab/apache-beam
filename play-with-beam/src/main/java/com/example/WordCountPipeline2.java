package com.example;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;


class Tokenize extends PTransform<PCollection<String>, PCollection<String>> {
    // factory method, to create a new instance of the class
    public static Tokenize of() {
        return new Tokenize();
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


class PrintElements extends PTransform<PCollection<String>, PCollection<String>> {
    public static PrintElements of() {
        return new PrintElements();
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

public class WordCountPipeline2 {

    public static void main(String[] args) {
        
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline
            .apply(TextIO.read().from("/Users/nag/apache-beam/play-with-beam/input.txt"))
            //.apply(FlatMapElements.into(TypeDescriptors.strings()).via(line->Arrays.asList(line.split("[^\\p{L}]"))))
            .apply(Tokenize.of())
            .apply(Filter.by(word->!word.isEmpty()))
            .apply(Count.perElement()) 
            .apply(MapElements.into(TypeDescriptors.strings()).via(wordCount->wordCount.getKey()+": "+wordCount.getValue()))
            //.apply(TextIO.write().to("wordscount-output").withoutSharding().withSuffix(".txt"));
            .apply(PrintElements.of());
            
        pipeline.run().waitUntilFinish();


    }
    
}
