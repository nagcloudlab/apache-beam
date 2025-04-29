package com.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.lang.reflect.Type;
import java.util.List;

public class Ex5 {
    public static void main(String[] args) {

        // list of users json, use multiline string
        String jsonString = """
                [
                    {"name":"Alice", "age":30, "isStudent":false, "address":{"street":"123 Main St", "city":"Springfield"}},
                    {"name":"Bob", "age":25, "isStudent":true, "address":{"street":"456 Elm St", "city":"Shelbyville"}}
                ]
                """;
        ObjectMapper mapper = new ObjectMapper();
        try {
            //User[] users = mapper.readValue(jsonString, User[].class);
            //List<User> users = mapper.readValue(jsonString, mapper.getTypeFactory().constructCollectionType(List.class, User.class));
            List<User> users = mapper.readValue(jsonString, new TypeReference<List<User>>() {});
            for (User user : users) {
                System.out.println(user.name);   // Alice, Bob
                System.out.println(user.age);    // 30, 25
                System.out.println(user.isStudent); // false, true
                System.out.println(user.address.street); // 123 Main St, 456 Elm St
                System.out.println(user.address.city);   // Springfield, Shelbyville
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
