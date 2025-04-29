package com.example.day2.json;

import com.example.day2.json.model.Address;
import com.example.day2.json.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Ex4 {

    public static void main(String[] args) {

        User user = new User();
        user.setName("Riya");
        user.setAge(10);
        user.setStudent(true);
        Address address = new Address();
        address.setStreet("123 Main St");
        address.setCity("Springfield");
        address.setState("IL");
        address.setCountry("USA");
        user.setAddress(address);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String jsonString = objectMapper.writeValueAsString(user);
            System.out.println(jsonString);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
