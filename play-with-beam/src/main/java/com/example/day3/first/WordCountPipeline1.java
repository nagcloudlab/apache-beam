package com.example.day3.first;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;

public class WordCountPipeline1 {

    public static void main(String[] args) {
        
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline
            .apply(TextIO.read().from("/Users/nag/apache-beam/play-with-beam/input.txt"))
            .apply(FlatMapElements.into(TypeDescriptors.strings()).via(line->Arrays.asList(line.split("[^\\p{L}]"))))
            .apply(Filter.by(word->!word.isEmpty()))
            .apply(Count.perElement()) 
            .apply(MapElements.into(TypeDescriptors.strings()).via(wordCount->wordCount.getKey()+": "+wordCount.getValue()))
            .apply(TextIO.write().to("wordscount-output").withoutSharding().withSuffix(".txt"));
            
        pipeline.run().waitUntilFinish();


    }
    
}
