package com.example.day2.xml;

public class Ex2 {
    public static void main(String[] args) {

        // Create a User object
        com.example.day2.xml.model.User user = new com.example.day2.xml.model.User();
        user.setName("Riya");
        user.setAge(10);
        user.setStudent(true);

        // Convert User object to XML string
        try {
            javax.xml.bind.JAXBContext jaxbContext = javax.xml.bind.JAXBContext.newInstance(com.example.day2.xml.model.User.class);
            javax.xml.bind.Marshaller marshaller = jaxbContext.createMarshaller();
            marshaller.setProperty(javax.xml.bind.Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            marshaller.marshal(user, System.out);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
