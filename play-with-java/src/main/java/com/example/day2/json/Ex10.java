package com.example.day2.json;

import com.example.day2.json.model.Address;
import com.example.day2.json.model.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Ex10 {
    public static void main(String[] args) {

        ObjectMapper objectMapper = new ObjectMapper();

        Employee employee = new Employee();
        employee.setFullName("John Doe");
        employee.setAge(30);
        employee.setSalary(50000);
        Address address = new Address();
        address.setStreet("123 Main St");
        address.setCity("Springfield");
        address.setState("IL");
        address.setCountry("USA");
        employee.setAddress(address);
        employee.setDateOfBirth(new java.util.Date());

        try {
            String jsonString = objectMapper.writeValueAsString(employee);
            System.out.println(jsonString);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
