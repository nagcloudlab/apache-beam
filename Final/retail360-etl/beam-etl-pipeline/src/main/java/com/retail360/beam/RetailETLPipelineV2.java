package com.retail360.beam;

import com.retail360.beam.model.Customer;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetailETLPipelineV2 {

    private static final Logger logger = LoggerFactory.getLogger(RetailETLPipeline.class);

    // Define tags for main (valid) and side (invalid) outputs
    public static final TupleTag<Customer> validTag = new TupleTag<Customer>() {};
    public static final TupleTag<String> invalidTag = new TupleTag<String>() {};

    // Customer model
    public static class Customer {
        String customerId;
        String firstName;
        String lastName;
        String email;
        String country;
        String status;

        public Customer(String customerId, String firstName, String lastName, String email, String country, String status) {
            this.customerId = customerId;
            this.firstName = firstName;
            this.lastName = lastName;
            this.email = email;
            this.country = country;
            this.status = status;
        }

        @Override
        public String toString() {
            return String.join(",", customerId, firstName, lastName, email, country, status);
        }
    }

    // ParseFn with multi-output support
    public static class ParseFn extends DoFn<String, Customer> {
        @ProcessElement
        public void processElement(@Element String line, MultiOutputReceiver out) {
            try {
                String[] parts = line.split(",");
                if (parts.length != 6 || line.startsWith("customer_id")) {
                    out.get(invalidTag).output(line);
                    return;
                }
                Customer customer = new Customer(
                        parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
                );
                out.get(validTag).output(customer);
            } catch (Exception e) {
                logger.error("❌ Parse error for line: {}", line, e);
                out.get(invalidTag).output(line);
            }
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        options.setRunner(DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);

        String inputFile = "src/main/resources/cleaned_customers.csv";

        PCollectionTuple parsedResults = pipeline
                .apply("ReadCSV", TextIO.read().from(inputFile))
                .apply("ParseWithValidation", ParDo.of(new ParseFn())
                        .withOutputTags(validTag, TupleTagList.of(invalidTag)));

        // Valid customers
        PCollection<Customer> validCustomers = parsedResults.get(validTag);

        // Invalid rows
        PCollection<String> invalidLines = parsedResults.get(invalidTag);

        // Write valid customers to CSV
        validCustomers
                .apply("FormatValidCSV", ParDo.of(new DoFn<Customer, String>() {
                    @ProcessElement
                    public void processElement(@Element Customer customer, OutputReceiver<String> out) {
                        out.output(customer.toString());
                    }
                }))
                .apply("WriteValidCSV", TextIO.write()
                        .to("output/enriched_customers.csv")
                        .withSuffix(".csv")
                        .withNumShards(1));

        // Write invalid lines to a separate file
        invalidLines
                .apply("WriteInvalidCSV", TextIO.write()
                        .to("output/invalid_customers.csv")
                        .withSuffix(".csv")
                        .withNumShards(1));

        pipeline.run().waitUntilFinish();
    }
}
