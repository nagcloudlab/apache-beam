package com.example.chapter2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.VarInt;


import java.io.*;
import java.nio.charset.StandardCharsets;

public class BeamCoderExample {

    // Custom Coder that reverses strings during encoding/decoding
    public static class ReverseStringCoder extends CustomCoder<String> {

        @Override
        public void encode(String value, OutputStream outStream) throws IOException {
            String reversed = new StringBuilder(value).reverse().toString();
            byte[] bytes = reversed.getBytes(StandardCharsets.UTF_8);
            VarInt.encode(bytes.length, outStream);
            outStream.write(bytes);
        }

        @Override
        public String decode(InputStream inStream) throws IOException {
            int len = VarInt.decodeInt(inStream);
            byte[] bytes = new byte[len];
            inStream.read(bytes);
            String reversed = new String(bytes, StandardCharsets.UTF_8);
            return new StringBuilder(reversed).reverse().toString();
        }
    }

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create());

        // Step 1: Create a simple PCollection<String>
        PCollection<String> words = pipeline.apply("CreateInput", Create.of("one", "two", "three"))
                .setCoder(StringUtf8Coder.of()); // manually setting coder

        // Step 2: Use TypeDescriptor to map to KV<String, String>
        PCollection<KV<String, String>> kvWords = words.apply("MapToKV",
                MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via(word -> KV.of(word, word.toUpperCase())));

        // Step 3: Apply another transform and set a custom coder
        PCollection<String> reversed = words.apply("ReverseWords", MapElements
                        .into(TypeDescriptors.strings())
                        .via(word -> word))
                .setCoder(new ReverseStringCoder());

        // Step 4: Register the custom coder in the CoderRegistry
        pipeline.getCoderRegistry().registerCoderForType(
                new TypeDescriptor<String>() {}, new ReverseStringCoder());

        // Step 5: Print output
        kvWords.apply("PrintKV", ParDo.of(new DoFn<KV<String, String>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                //System.out.println("KV: " + c.element());
            }
        }));

        reversed.apply("PrintReversed", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println("Reversed Output: " + c.element());
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
