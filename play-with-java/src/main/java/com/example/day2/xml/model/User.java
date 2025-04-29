package com.example.day2.xml.model;

import lombok.Data;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@Data
public class User {
    private String name;
    private int age;
    private boolean isStudent;
}
