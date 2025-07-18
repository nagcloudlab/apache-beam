package com.example.day1;

import com.example.greetingservice.HelloRequest;
import com.example.greetingservice.HelloResponse;
import com.example.greetingservice.HelloServiceGrpc;
import org.springframework.grpc.server.service.GrpcService;

@GrpcService
public class GrpcHelloServiceImpl extends HelloServiceGrpc.HelloServiceImplBase {

    @Override
    public void sayHello(HelloRequest request, io.grpc.stub.StreamObserver<HelloResponse> responseObserver) {
        System.out.println("Received request: " + request.getName());
        String greeting = "Hello " + request.getName();
        HelloResponse response = HelloResponse.newBuilder().setMessage(greeting).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
