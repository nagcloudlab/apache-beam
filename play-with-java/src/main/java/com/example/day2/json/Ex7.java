package com.example.day2.json;

import com.example.day2.json.model.User;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

public class Ex7 {

    public static void main(String[] args) {

        // List of users
        String jsonString = """
                [
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
                    },
                    {
                        "name": "John",
                        "age": 20,
                        "isStudent": false,
                        "address": {
                            "street": "456 Elm St",
                            "city": "Springfield",
                            "state": "IL",
                            "country": "USA"
                        }
                    }
                ]
                """;

        ObjectMapper objectMapper = new ObjectMapper();

        try {
            //User[] users = objectMapper.readValue(jsonString, User[].class);
            //List<User> users = objectMapper.readValue(jsonString, objectMapper.getTypeFactory().constructCollectionType(List.class, User.class));
            List<User> users=objectMapper.readValue(jsonString, new TypeReference<List<User>>() {});
            for (User user : users) {
                System.out.println(user.getName());
                System.out.println(user.getAge());
                System.out.println(user.isStudent());
                System.out.println(user.getAddress().getStreet());
                System.out.println(user.getAddress().getCity());
                System.out.println(user.getAddress().getState());
                System.out.println(user.getAddress().getCountry());
            }
        } catch (Exception e) {
            e.printStackTrace();

        }

    }

}
