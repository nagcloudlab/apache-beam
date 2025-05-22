package com.retail360.model;

public class Customer {
    private String customerId;
    private String firstName;
    private String lastName;
    private String email;
    private String country;
    private String status;

    public Customer() {}

    public Customer(String customerId, String firstName, String lastName,
                    String email, String country, String status) {
        this.customerId = customerId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.country = country;
        this.status = status;
    }

    public String getCustomerId() {
        return customerId;
    }
    public String getFirstName() {
        return firstName;
    }
    public String getLastName() {
        return lastName;
    }
    public String getEmail() {
        return email;
    }
    public String getCountry() {
        return country;
    }
    public String getStatus() {
        return status;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }
    public void setEmail(String email) {
        this.email = email;
    }
    public void setCountry(String country) {
        this.country = country;
    }
    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return customerId + "," + firstName + "," + lastName + "," +
               email + "," + country + "," + status;
    }
}
