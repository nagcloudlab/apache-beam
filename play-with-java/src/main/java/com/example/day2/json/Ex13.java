package com.example.day2.json;

import com.example.day2.json.model.Book;
import com.example.day2.json.model.BookMixin;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Ex13 {
    public static void main(String[] args) {

        Book book = new Book("The Great Gatsby", "F. Scott Fitzgerald", 1925);

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.addMixIn(Book.class, BookMixin.class);

        try {
            // Serialize the Book object to JSON
            String jsonString = objectMapper.writeValueAsString(book);
            System.out.println("Serialized JSON: " + jsonString);

            // Deserialize the JSON back to a Book object
            Book deserializedBook = objectMapper.readValue(jsonString, Book.class);
            System.out.println("Deserialized Book: " + deserializedBook);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
