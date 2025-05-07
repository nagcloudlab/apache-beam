package com.example.bytes;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

class Employee implements Comparable<Employee> {
    public int id;
    public String name;
    public double salary;

    public Employee(int id, String name, double salary) {
        this.id = id;
        this.name = name;
        this.salary = salary;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", salary=" + salary +
                '}';
    }

    @Override
    public int compareTo(Employee o) {
        return Integer.compare(this.id, o.id);
    }

}

public class CompareExample {
    public static void main(String[] args) {

        List<Employee> employees = new ArrayList<>();
        employees.add(new Employee(1, "John", 4000));
        employees.add(new Employee(5, "Joe", 5000));
        employees.add(new Employee(2, "Jane", 5000));
        employees.add(new Employee(4, "Jill", 4000));
        employees.add(new Employee(3, "Jack", 6000));

        //--------------------------------------
        // sorting
        // min
        // max
        //---------------------------------------

        Collections.sort(employees);

        System.out.println("Sorted Employees:");
        for (Employee employee : employees) {
            System.out.println(employee);
        }

        // sorting by salary
        Comparator<Employee> bySalary = (e1, e2) -> Double.compare(e1.salary, e2.salary);
        Collections.sort(employees, bySalary);

        System.out.println("Sorted Employees by Salary:");
        for (Employee employee : employees) {
            System.out.println(employee);
        }

        // sorting by name
        Comparator<Employee> byName = (e1, e2) -> e1.name.compareTo(e2.name);
        Collections.sort(employees, byName);

        System.out.println("Sorted Employees by Name:");
        for (Employee employee : employees) {
            System.out.println(employee);
        }

        // sort by salary and then by name
        Comparator<Employee> bySalaryAndName = bySalary.thenComparing(byName);
        Collections.sort(employees, bySalaryAndName);
        System.out.println("Sorted Employees by Salary and Name:");
        for (Employee employee : employees) {
            System.out.println(employee);
        }


        // fins employee with max salary
        Employee maxSalaryEmployee = Collections.max(employees, bySalary);
        System.out.println("Employee with max salary: " + maxSalaryEmployee);

    }
}
