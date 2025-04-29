package com.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Ex2 {
    public static void main(String[] args) throws JsonProcessingException {
        User user = new User();
        user.name = "Bob";
        user.age = 25;
        user.isStudent = true;

        ObjectMapper mapper = new ObjectMapper();
        String jsonOutput = mapper.writeValueAsString(user);

        System.out.println(jsonOutput);// {"name":"Bob","age":25,"isStudent":true}

    }
}
