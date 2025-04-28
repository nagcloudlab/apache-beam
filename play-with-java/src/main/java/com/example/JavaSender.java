package com.example;

import java.net.URI;
import java.net.http.HttpRequest;

public class JavaSender {

    public static void main(String[] args) {

        // JSON payload
        String jsonPayload = "{ \"name\": \"John\", \"age\": 30, \"city\": \"New York\" }";

        // Create HttpRequest request
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8181/api/employee"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                .build();

        // Send the request and get the response
        try {
            var response = java.net.http.HttpClient.newHttpClient()
                    .send(request, java.net.http.HttpResponse.BodyHandlers.ofString());

            // Print the response
            System.out.println("Response code: " + response.statusCode());
            System.out.println("Response body: " + response.body());

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
