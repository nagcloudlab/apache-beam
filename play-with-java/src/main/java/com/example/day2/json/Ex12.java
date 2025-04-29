package com.example.day2.json;

import com.example.day2.json.model.Product;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Ex12 {
    public static void main(String[] args) {

        String productJsonString= """
        {
            "name": "Laptop",
            "category": "Electronics"
        }
                """;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            Product product = objectMapper.readValue(productJsonString, Product.class);
            System.out.println("Product Name: " + product.getName());
            System.out.println("Product Price: " + product.getPrice());
            if(product.getPrice() == null) {
                System.out.println("Product Price is not available");
            } else {
                System.out.println("Product Price: " + product.getPrice());
            }
            System.out.println("Product Category: " + product.getCategory());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
