package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Ex3 {
    public static void main(String[] args) throws Exception {

       // user with address
        String jsonString = "{\"name\":\"Alice\", \"age\":30, \"isStudent\":false, \"address\":{\"street\":\"123 Main St\", \"city\":\"Springfield\"}}";

        ObjectMapper mapper = new ObjectMapper();
        User user = mapper.readValue(jsonString, User.class);

        System.out.println(user.name);   // Alice
        System.out.println(user.age);    // 30
        System.out.println(user.isStudent); // false
        System.out.println(user.address.street); // 123 Main St
        System.out.println(user.address.city);   // Springfield

    }
}
