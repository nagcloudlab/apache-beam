package com.example.day1;

import com.example.greetingservice.HelloRequest;
import com.example.greetingservice.HelloResponse;
import com.example.greetingservice.HelloServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class GrpcClientExample {
    public static void main(String[] args) {

            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9090)
                    .usePlaintext()
                    .build();

            HelloServiceGrpc.HelloServiceBlockingStub stub = HelloServiceGrpc.newBlockingStub(channel);

            // Create a request
            HelloRequest request = HelloRequest.newBuilder()
                    .setName("Riya")
                    .build();

            // Call the service
            HelloResponse response = stub.sayHello(request);

            // Print the response
            System.out.println("Response from server: " + response.getMessage());
        }
}
