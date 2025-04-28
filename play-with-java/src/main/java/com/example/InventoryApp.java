package com.example;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import com.example.model.Apple;



public class InventoryApp {
    public static void main(String[] args) {

        List<Apple> appleInventory=List.of(
            new Apple("green", 150),
            new Apple("red", 100),
            new Apple("yellow", 200),
            new Apple("green", 120),
            new Apple("red", 130)
        );

        // Req-1 : filter the apples which are green
        System.out.println(
           filterApples(appleInventory, apple-> "green".equals(apple.getColor()))
        );

        // Req-2 : filter the apples which are red
        System.out.println(
            filterApples(appleInventory, apple-> "red".equals(apple.getColor()))
        );

        // Req-3 : filter the apples which are greater than 120
        System.out.println(
            filterApples(appleInventory, apple-> apple.getWeight() > 120)
        );
      
    }

    //----------------------------------------------------------------
    // declarative style
    //----------------------------------------------------------------
    // intention + implementation separated
    // how?
    // by parameterizing the filter method
    // - by value
    // - by object
    // - by function  aka functional programming   ( compact & concise code)
    public static List<Apple> filterApples(List<Apple> appleInventory, Predicate<Apple> predicate) {
        List<Apple> filteredApples=new ArrayList<>();
        for (Apple apple : appleInventory) {
            if (predicate.test(apple)) {
                filteredApples.add(apple);
            }
        }
        return filteredApples;
    }



    //---------------------------------------------------------------
    // imperative style
    //----------------------------------------------------------------

    // intention + implementation mixed 
    public static List<Apple> filterGreenApples(List<Apple> appleInventory) {
        List<Apple> greeApples=new ArrayList<>();
        for (Apple apple : appleInventory) {
            if (apple.getColor().equals("green")) {
                greeApples.add(apple);
            }
        }
        return greeApples;
    }

    // intention + implementation mixed
    public static List<Apple> filterRedApples(List<Apple> appleInventory) {
        List<Apple> redApples=new ArrayList<>();
        for (Apple apple : appleInventory) {
            if (apple.getColor().equals("red")) {
                redApples.add(apple);
            }
        }
        return redApples;
    }
}