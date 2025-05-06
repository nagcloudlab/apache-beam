package com.example.day5.transforms.agg;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;


public class MaxMinExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Sample data for global min/max
        PCollection<Integer> scores = pipeline.apply("CreateScores",
                org.apache.beam.sdk.transforms.Create.of(88, 92, 76, 99, 85));

        // ✅ Max.globally()
        PCollection<Integer> maxScore = scores.apply("MaxGlobally", Max.integersGlobally());
        maxScore.apply("PrintMaxGlobally", MapElements.via(new SimpleFunction<Integer, Void>() {
            @Override
            public Void apply(Integer input) {
                System.out.println("Highest Score: " + input);
                return null;
            }
        }));

        // ✅ Min.globally()
        PCollection<Integer> minScore = scores.apply("MinGlobally", Min.integersGlobally());
        minScore.apply("PrintMinGlobally", MapElements.via(new SimpleFunction<Integer, Void>() {
            @Override
            public Void apply(Integer input) {
                System.out.println("Lowest Score: " + input);
                return null;
            }
        }));

        // Sample keyed data for perKey min/max
        PCollection<KV<String, Integer>> userScores = pipeline.apply("CreateUserScores",
                org.apache.beam.sdk.transforms.Create.of(
                        KV.of("Alice", 88),
                        KV.of("Alice", 92),
                        KV.of("Bob", 76),
                        KV.of("Bob", 99),
                        KV.of("Alice", 85)
                ));

        // ✅ Max.perKey()
        PCollection<KV<String, Integer>> maxPerUser = userScores.apply("MaxPerUser", Max.integersPerKey());
        maxPerUser.apply("PrintMaxPerUser", MapElements.via(new SimpleFunction<KV<String, Integer>, Void>() {
            @Override
            public Void apply(KV<String, Integer> input) {
                System.out.println("User: " + input.getKey() + " → Max Score: " + input.getValue());
                return null;
            }
        }));

        // ✅ Min.perKey()
        PCollection<KV<String, Integer>> minPerUser = userScores.apply("MinPerUser", Min.integersPerKey());
        minPerUser.apply("PrintMinPerUser", MapElements.via(new SimpleFunction<KV<String, Integer>, Void>() {
            @Override
            public Void apply(KV<String, Integer> input) {
                System.out.println("User: " + input.getKey() + " → Min Score: " + input.getValue());
                return null;
            }
        }));



        PCollection<KV<String,Product>> products = pipeline.apply("CreateProducts",
                org.apache.beam.sdk.transforms.Create.of(
                        KV.of("Electronics", new Product("Laptop", 1200, 100)),
                        KV.of("Electronics", new Product("Smartphone", 800, 50)),
                        KV.of("Furniture", new Product("Sofa", 500, 200)),
                        KV.of("Furniture", new Product("Table", 300, 50))
                ));

        // ✅ Max.perKey() for products
        PCollection<KV<String, Product>> maxProductPerCategory = products.apply("MaxProductPerCategory", Max.<String, Product>perKey());
        maxProductPerCategory.apply("PrintMaxProductPerCategory", MapElements.via(new SimpleFunction<KV<String, Product>, Void>() {
            @Override
            public Void apply(KV<String, Product> input) {
                System.out.println("Category: " + input.getKey() + " → Most Expensive Product: " + input.getValue().getName() + " ($" + input.getValue().getPrice() + ")");
                return null;
            }
        }));

        // FInd max product by discount using custom comparator

//        PCollection<KV<String, Product>> maxDiscountProductPerCategory = products.apply("MaxDiscountProductPerCategory", Max.<String, Product,>perKey(new Comparator<Product>() {
//            @Override
//            public int compare(Product p1, Product p2) {
//                return Integer.compare(p1.getDiscount(), p2.getDiscount());
//            }
//        }));

//        maxDiscountProductPerCategory.apply("PrintMaxDiscountProductPerCategory", MapElements.via(new SimpleFunction<KV<String, Product>, Void>() {
//            @Override
//            public Void apply(KV<String, Product> input) {
//                System.out.println("Category: " + input.getKey() + " → Highest Discount Product: " + input.getValue().getName() + " (" + input.getValue().getDiscount() + "% off)");
//                return null;
//            }
//        }));


        pipeline.run().waitUntilFinish();
    }
}


class Product implements Serializable, Comparable<Product> {
    private String name;
    private int price;
    private int discount;

    public Product(String name, int price, int discount) {
        this.name = name;
        this.price = price;
        this.discount = discount;
    }

    public String getName() {
        return name;
    }
    public int getPrice() {
        return price;
    }
    public int getDiscount() {
        return discount;
    }
    @Override
    public int compareTo(Product other) {
        return Integer.compare(this.price, other.price);
    }

}

