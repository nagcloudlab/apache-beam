// 📁 com.retail360.beam.fn.TransformFn.java
package com.retail360.beam.transform;

import com.retail360.beam.model.Customer;
import org.apache.beam.sdk.transforms.DoFn;

public class TransformFn extends DoFn<Customer, Customer> {
    @ProcessElement
    public void processElement(@Element Customer customer, OutputReceiver<Customer> out) {
        String upperCountry = customer.country.toUpperCase();
        out.output(new Customer(
                customer.customerId,
                customer.firstName,
                customer.lastName,
                customer.email,
                upperCountry,
                customer.status
        ));
    }
}
