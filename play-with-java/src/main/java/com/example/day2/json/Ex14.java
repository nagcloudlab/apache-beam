package com.example.day2.json;

import com.example.day2.json.model.Employee;
import com.example.day2.json.views.Views;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Ex14 {
    public static void main(String[] args) {
        Employee employee = new Employee();
        employee.setFullName("John Doe");
        employee.setAge(10);
        employee.setSalary(1000.0);


        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String jsonString = objectMapper.writerWithView(Views.Internal.class).writeValueAsString(employee);
            System.out.println(jsonString);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
