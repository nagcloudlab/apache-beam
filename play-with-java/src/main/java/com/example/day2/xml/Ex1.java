package com.example.day2.xml;

import com.example.day2.xml.model.User;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

public class Ex1 {

    public static void main(String[] args) throws JAXBException {


        String xmlString = """
                <user>
                    <name>Riya</name>
                    <age>10</age>
                    <isStudent>true</isStudent>
                </user>
                """;

        // Parse XML string to User object
        JAXBContext jaxbContext = JAXBContext.newInstance(User.class);

        User user = (User) jaxbContext.createUnmarshaller().unmarshal(new java.io.StringReader(xmlString));

        // Print User object properties
        System.out.println(user.getName());
        System.out.println(user.getAge());
        System.out.println(user.isStudent());

    }

}
