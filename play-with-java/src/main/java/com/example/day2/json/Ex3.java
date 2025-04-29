package com.example.day2.json;

import com.example.day2.json.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Ex3 {
    public static void main(String[] args) {


        String jsonString = """
                {
                    "name": "Riya",
                    "age": 10,
                    "isStudent": true,
                    "address": {
                        "street": "123 Main St",
                        "city": "Springfield",
                        "state": "IL",
                        "country": "USA"
                    }
                }
                """;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            User user = objectMapper.readValue(jsonString, User.class);
            System.out.println(user.getName());
            System.out.println(user.getAge());
            System.out.println(user.isStudent());
            System.out.println(user.getAddress().getStreet());
            System.out.println(user.getAddress().getCity());
            System.out.println(user.getAddress().getState());
            System.out.println(user.getAddress().getCountry());
        } catch (Exception e) {
            e.printStackTrace();
        }



    }
}
