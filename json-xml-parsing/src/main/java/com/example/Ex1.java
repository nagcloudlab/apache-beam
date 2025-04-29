package com.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Ex1 {
    public static void main(String[] args) throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();

        String jsonString = "{\"name\":\"Alice\", \"age\":30, \"isStudent\":false}";

        User user = mapper.readValue(jsonString, User.class);

        System.out.println(user.name);   // Alice
        System.out.println(user.age);    // 30
        System.out.println(user.isStudent); // false


    }
}
