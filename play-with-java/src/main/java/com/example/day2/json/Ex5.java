package com.example.day2.json;

import com.example.day2.json.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

public class Ex5 {
    public static void main(String[] args) {

        User user = new User();
        user.setName("Riya");
        user.setAge(10);
        user.setStudent(true);
        user.setCourses(List.of("Math", "Science", "English"));

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String jsonString = objectMapper.writeValueAsString(user);
            System.out.println(jsonString);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
