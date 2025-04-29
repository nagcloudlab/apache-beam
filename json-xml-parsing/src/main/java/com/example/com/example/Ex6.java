package com.example.com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Ex6 {
    public static void main(String[] args) {

        // parsing without POJO class

        String jsonString = "{\"name\":\"Alice\", \"age\":30, \"isStudent\":false, \"address\":{\"street\":\"123 Main St\", \"city\":\"Springfield\"}}";
        ObjectMapper mapper = new ObjectMapper();

        try {
            JsonNode jsonNode = mapper.readTree(jsonString);
            String name = jsonNode.get("name").asText();
            int age = jsonNode.get("age").asInt();
            boolean isStudent = jsonNode.get("isStudent").asBoolean();
            String street = jsonNode.get("address").get("street").asText();
            String city = jsonNode.get("address").get("city").asText();
            System.out.println(name);   // Alice
            System.out.println(age);    // 30
            System.out.println(isStudent); // false
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
