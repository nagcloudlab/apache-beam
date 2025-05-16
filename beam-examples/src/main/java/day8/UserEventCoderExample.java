package com.example.chapter2;

import com.example.chapter2.coder.UserEventCoder;
import com.example.chapter2.event.UserEvent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.VarInt;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class UserEventCoderExample {


    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create());

        // ✅ Fix: Use withCoder() explicitly
        PCollection<UserEvent> events = pipeline
                .apply("CreateEvents", Create.of(
                                new UserEvent("user1", "login", 1688888888L),
                                new UserEvent("user2", "click", 1688888899L),
                                new UserEvent("user3", "logout", 1688888900L)
                        ));
                        //.withCoder(new UserEventCoder())); // 👈 Fix here

        PCollection<KV<String, String>> kvEvents = events.apply("MapToKV",
                MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via(event -> KV.of(event.userId, event.eventType)));

        kvEvents.apply("PrintKV", ParDo.of(new DoFn<KV<String, String>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println("User Event: " + c.element());
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
