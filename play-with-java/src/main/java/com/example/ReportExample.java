package com.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReportExample {

    public static void main(String[] args) throws IOException {

        // way-1: read csv file into memory,
        // calculate total salary of each department
        BufferedReader bufferedReader = new BufferedReader(
                new FileReader(new File("/home/nag/apache-beam/play-with-java/report.csv")));
        List<String> csvFile = new ArrayList<>();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            csvFile.add(line);
        }
        bufferedReader.close();

        Map<String, Double> departmentSalaryMap = new java.util.HashMap<>();
        int i = 0;
        for (String csvLine : csvFile) {
            if (i++ == 0) {
                continue; // Skip the header line
            }
            String[] columns = csvLine.split(",");
            String department = columns[1];
            double salary = Double.parseDouble(columns[2]);
            departmentSalaryMap.put(department, departmentSalaryMap.getOrDefault(department, 0.0) + salary);

        }
        // Print the total salary of each department
        for (Map.Entry<String, Double> entry : departmentSalaryMap.entrySet()) {
            System.out.println("Department: " + entry.getKey() + ", Total Salary: " + entry.getValue());
        }

        // way-2: read csv file line by line, by using stream api
        // calculate total salary of each department

        departmentSalaryMap = Files.lines(new File("/home/nag/apache-beam/play-with-java/report.csv").toPath()).skip(1) // Skip
                .map(line1 -> line1.split(",")).parallel()
                .collect(java.util.stream.Collectors.groupingBy(columns -> columns[1],
                        java.util.stream.Collectors.summingDouble(columns -> Double.parseDouble(columns[2])) // Sum the
                ));

        // Print the total salary of each department
        for (Map.Entry<String, Double> entry : departmentSalaryMap.entrySet()) {
            System.out.println("Department: " + entry.getKey() + ", Total Salary: " + entry.getValue());
        }

    }

}
