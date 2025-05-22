package com.retail360.beam.transform;

import com.retail360.beam.model.Customer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseFn extends DoFn<String, Customer> {

    public static final TupleTag<Customer> validTag = new TupleTag<Customer>() {};
    public static final TupleTag<String> invalidTag = new TupleTag<String>() {};

    private static final Logger logger = LoggerFactory.getLogger(ParseFn.class);

    @ProcessElement
    public void processElement(@Element String line, MultiOutputReceiver out) {
        try {
            String[] parts = line.split(",");
            if (parts.length != 6) {
                logger.warn("❌ Invalid line (wrong parts): {}", line);
                out.get(invalidTag).output(line);
                return;
            }

            Customer customer = new Customer(
                    parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
            );
            out.get(validTag).output(customer);

        } catch (Exception e) {
            logger.error("❌ Failed to parse line: {}", line, e);
            out.get(invalidTag).output(line);
        }
    }
}
