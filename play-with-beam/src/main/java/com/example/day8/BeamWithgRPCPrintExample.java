package com.example.day8;

import com.example.greetingservice.HelloRequest;
import com.example.greetingservice.HelloResponse;
import com.example.greetingservice.HelloServiceGrpc;
import com.example.model.Transaction;
import com.example.transforms.JsonToTransactionTransform;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;

public class BeamWithgRPCPrintExample {
    public static void main(String[] args) {
        // Step 1: Create the pipeline
        Pipeline pipeline = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create()
        );

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("group.id", "bean-pipeline-consumer-group-23");
        consumerProps.put("auto.offset.reset", "earliest");

        // Step 2: Read from Kafka topic
        var kafkaRecords = pipeline.apply("ReadFromKafka", KafkaIO.<String, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("topic1")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(consumerProps)
                .withoutMetadata() // we don't need Kafka metadata
        );

        // Step 3: Convert JSON string into Transaction object
        var transactions = kafkaRecords.apply("JsonToTransaction", ParDo.of(new DoFn<KV<String, String>, String>() {

            private ManagedChannel channel;
            HelloServiceGrpc.HelloServiceBlockingStub stub ;

            @Setup
            public void setup() {
                // Initialize gRPC channel here
                channel = ManagedChannelBuilder.forAddress("localhost", 9090)
                        .usePlaintext()
                        .build();
                stub = HelloServiceGrpc.newBlockingStub(channel);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                KV<String, String> record = c.element();
                String recordValue = record.getValue();
                // Create a request
                HelloRequest request = HelloRequest.newBuilder()
                        .setName(recordValue)
                        .build();
                // Call the service
                HelloResponse response = stub.sayHello(request);
                String outputValue = response.getMessage();
                c.output(outputValue);
            }
        }));

        // Step 4: Print Kafka-record timestamp used by Beam
        transactions.apply("PrintHelloValue", ParDo.of(new org.apache.beam.sdk.transforms.DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String recordValue = c.element().toString();
                System.out.println("Kafka-record timestamp used by Beam: " + recordValue);
            }
        }));

        // Step 5: Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}
