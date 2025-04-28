package com.example;

import com.example.greetingservice.HelloRequest;
import com.example.greetingservice.HelloResponse;
import com.example.greetingservice.HelloServiceGrpc;
import org.springframework.grpc.server.service.GrpcService;

@GrpcService
public class HelloServiceImpl extends HelloServiceGrpc.HelloServiceImplBase {

    @Override
    public void sayHello(HelloRequest request, io.grpc.stub.StreamObserver<HelloResponse> responseObserver) {
        String greeting = "Hello " + request.getName();
        HelloResponse response = HelloResponse.newBuilder().setMessage(greeting).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
