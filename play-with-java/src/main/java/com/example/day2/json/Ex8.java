package com.example.day2.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Ex8 {
    public static void main(String[] args) {

        String jsonString = """
                {
                    "name": "Riya",
                    "age": 10,
                    "isStudent": true
                }
                """;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode jsonNode = objectMapper.readTree(jsonString);
            System.out.println("Name: " + jsonNode.get("name").asText());
            System.out.println("Age: " + jsonNode.get("age").asInt());
            System.out.println("Is Student: " + jsonNode.get("isStudent").asBoolean());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
