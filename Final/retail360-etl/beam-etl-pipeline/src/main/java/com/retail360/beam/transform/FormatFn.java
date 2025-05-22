// 📁 com.retail360.beam.fn.FormatFn.java
package com.retail360.beam.transform;

import com.retail360.beam.model.Customer;
import org.apache.beam.sdk.transforms.DoFn;

public class FormatFn extends DoFn<Customer, String> {
    @ProcessElement
    public void processElement(@Element Customer customer, OutputReceiver<String> out) {
        String fullName = customer.firstName + " " + customer.lastName;
        String line = String.join(",",
                customer.customerId,
                fullName,
                customer.email,
                customer.country,
                customer.status
        );
        out.output(line);
    }
}
