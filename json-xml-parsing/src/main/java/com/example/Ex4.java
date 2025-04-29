package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

public class Ex4 {
    public static void main(String[] args) {

        User user = new User();
        user.name = "Alice";
        user.age = 30;
        user.isStudent = false;
        user.address = new Address();
        user.address.street = "123 Main St";
        user.address.city = "Springfield";
        user.courses = List.of("Math", "Science", "History");

        ObjectMapper mapper = new ObjectMapper();
        try {
            String jsonString = mapper.writeValueAsString(user);
            System.out.println(jsonString);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
