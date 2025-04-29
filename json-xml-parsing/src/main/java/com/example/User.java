package com.example;


import lombok.Data;

import java.util.List;

@Data
public class User {
    public String name;
    public int age;
    public boolean isStudent;
    public Address address;
    public List<String> courses;
}
