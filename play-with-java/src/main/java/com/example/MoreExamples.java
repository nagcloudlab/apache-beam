package com.example;

import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.example.model.Dish;

public class MoreExamples {

    public static void main(String[] args) {
        List<Dish> menu = Dish.menu;

        // 1. Grouping by type
        Map<Dish.Type, List<Dish>> dishesByType =  // <1>
        menu.stream()
        .collect(Collectors.groupingBy(Dish::getType));
        System.out.println(dishesByType);

        // 2. Partitioning by vegetarian
        Map<Boolean, List<Dish>> dishesByVegetarian =  // <2>
        menu.stream()
        .collect(Collectors.partitioningBy(Dish::isVegetarian));
        System.out.println(dishesByVegetarian);

        // 3. Grouping by type and counting
        Map<Dish.Type, Long> typesCount =  // <3>
        menu.stream()
        .collect(Collectors.groupingBy(Dish::getType, Collectors.counting()));
        System.out.println(typesCount);

        // 4. Grouping by type and summing calories
        Map<Dish.Type, Integer> typesCalories =  // <4>
        menu.stream()
        .collect(Collectors.groupingBy(Dish::getType, Collectors.summingInt(Dish::getCalories)));
        System.out.println(typesCalories);

        // 5. Summarizing calories
        IntSummaryStatistics caloriesSummary =  // <5>
        menu.stream()
        .collect(Collectors.summarizingInt(Dish::getCalories));
        System.out.println(caloriesSummary);


        // is vegetarian friendly
        boolean isVegetarianFriendly = menu.stream()
                .anyMatch(Dish::isVegetarian);
        System.out.println("Is vegetarian friendly: " + isVegetarianFriendly);




    }
    
}
