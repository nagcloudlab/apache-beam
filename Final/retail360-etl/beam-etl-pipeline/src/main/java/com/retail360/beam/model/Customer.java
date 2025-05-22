// 📁 com.retail360.beam.model.Customer.java
package com.retail360.beam.model;


import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class Customer {
    public String customerId;
    public String firstName;
    public String lastName;
    public String email;
    public String country;
    public String status;

    // Default constructor for Avro serialization
    public Customer() {
    }

    public Customer(String customerId, String firstName, String lastName, String email, String country, String status) {
        this.customerId = customerId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.country = country;
        this.status = status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Customer customer = (Customer) o;
        return customerId.equals(customer.customerId) &&
                firstName.equals(customer.firstName) &&
                lastName.equals(customer.lastName) &&
                email.equals(customer.email) &&
                country.equals(customer.country) &&
                status.equals(customer.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(customerId, firstName, lastName, email, country, status);
    }

    public String getStatus() {
        return status;
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
}
