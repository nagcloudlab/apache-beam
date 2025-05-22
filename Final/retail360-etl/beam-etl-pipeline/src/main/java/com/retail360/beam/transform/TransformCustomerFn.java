package com.retail360.beam.transform;

import com.retail360.beam.model.Customer;
import org.apache.beam.sdk.transforms.DoFn;

public class TransformCustomerFn extends DoFn<Customer, String> {

    @ProcessElement
    public void processElement(@Element Customer customer, OutputReceiver<String> out) {
        String fullName = customer.getFirstName() + " " + customer.getLastName();
        String enriched = String.format(
                "%s,%s,%s,%s,%s,%s",
                customer.getCustomerId(),
                fullName,
                customer.getEmail(),
                customer.getCountry(),
                customer.getStatus(),
                "ENRICHED" // optional metadata
        );
        out.output(enriched);
    }
}
