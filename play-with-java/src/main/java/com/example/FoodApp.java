package com.example;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.example.model.Dish;

public class FoodApp {

    public static void main(String[] args) {

        List<Dish> menu=Dish.menu;
        // get low calories dishes (calories < 400) , dish names sorted by calories

        System.out.println(
            getLowCaloriesDishes_v1(menu)
        );

        System.out.println(
            getLowCaloriesDishes_v2(menu)
        );
        
    }

    public static List<String> getLowCaloriesDishes_v2(List<Dish> menu) {
        // step-1 : filter the dishes with calories < 400
        // step-2 : sort the dishes by calories
        // step-3 : map the dishes to their names
        // step-4 : return the dish names
        return menu.stream()
                .parallel()
                .filter(dish -> dish.getCalories() < 400)
                .sorted(Comparator.comparingInt(Dish::getCalories))
                .map(Dish::getName) // Method Reference
                .toList();
    }

    public static List<String> getLowCaloriesDishes_v1(List<Dish> menu) {
       // step-1 : filter the dishes with calories < 400
       List<Dish> lowCaloriesDishes = new ArrayList<>();
         for (Dish dish : menu) {
              if (dish.getCalories() < 400) {
                lowCaloriesDishes.add(dish);
              }
         }
       // step-2 : sort the dishes by calories
        lowCaloriesDishes.sort(new Comparator<Dish>() {
            @Override
            public int compare(Dish o1, Dish o2) {
                return Integer.compare(o1.getCalories(), o2.getCalories());
            }
        });
       // step-3 : map the dishes to their names
       List<String> dishNames = new ArrayList<>();
        for (Dish dish : lowCaloriesDishes) {
            dishNames.add(dish.getName());
        }
       // step-4 : return the dish names
        return dishNames;
    }
    
}
