package com.retail360.beam.transform;


import com.retail360.beam.model.Customer;
import org.apache.beam.sdk.transforms.DoFn;

public class FilterActiveFn extends DoFn<Customer, Customer> {

    @ProcessElement
    public void processElement(@Element Customer customer, OutputReceiver<Customer> out) {
        if ("ACTIVE".equalsIgnoreCase(customer.getStatus())) {
            out.output(customer);
        }
    }
}
