package com.example.day2.json;

import com.example.day2.json.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Ex2 {
    public static void main(String[] args) {


        User user = new User();
        user.setName("Riya");
        user.setAge(10);
        user.setStudent(true);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String jsonString = objectMapper.writeValueAsString(user);
            System.out.println(jsonString);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
