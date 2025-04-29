package com.example.day2.json;

import com.example.day2.json.model.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Ex1 {

    public static void main(String[] args) throws JsonProcessingException {

        String jsonString = """
                {
                    "name": "Riya",
                    "age": 10,
                    "isStudent": true
                }
                """;

        ObjectMapper objectMapper = new ObjectMapper();
        User user = objectMapper.readValue(jsonString, User.class);
        System.out.println(user.name);
        System.out.println(user.age);
        System.out.println(user.isStudent);

    }

}
