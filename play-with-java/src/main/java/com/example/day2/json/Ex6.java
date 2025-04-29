package com.example.day2.json;

import com.example.day2.json.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Ex6 {
    public static void main(String[] args) {


        String jsonString = """
                {
                    "name": "Riya",
                    "age": 10,
                    "isStudent": true,
                    "courses": ["Math", "Science", "English"],
                    "address": {
                        "street": "123 Main St",
                        "city": "New York",
                        "state": "NY"
                    }
                }
                """;

        // Convert User object to JSON string
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            User user = objectMapper.readValue(jsonString, User.class);
            System.out.println(user.getName());
            System.out.println(user.getAge());
            System.out.println(user.isStudent());
            System.out.println(user.getCourses());
            System.out.println(user.getAddress().getStreet());
            System.out.println(user.getAddress().getCity());
            System.out.println(user.getAddress().getState());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
