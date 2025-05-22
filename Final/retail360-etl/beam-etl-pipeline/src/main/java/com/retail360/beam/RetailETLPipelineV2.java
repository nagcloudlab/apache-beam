package com.retail360.beam;

import com.retail360.beam.model.Customer;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetailETLPipelineV2 {

    private static final Logger logger = LoggerFactory.getLogger(RetailETLPipelineV2.class);

    // Define tags for main (valid) and side (invalid) outputs
    public static final TupleTag<Customer> validTag = new TupleTag<Customer>() {};
    public static final TupleTag<String> invalidTag = new TupleTag<String>() {};

    // Customer model
    @DefaultCoder(AvroCoder.class)
    public static class Customer {
        String customerId;
        String firstName;
        String lastName;
        String email;
        String country;
        String status;

        // Constructor
        public Customer() {
            // Default constructor for Avro
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
        public String toString() {
            return String.join(",", customerId, firstName, lastName, email, country, status);
        }
    }

    // ParseFn with multi-output support
    public static class ParseFn extends DoFn<String, Customer> {
        @ProcessElement
        public void processElement(@Element String line, MultiOutputReceiver out) {
            line = line.trim(); // ✅ Trim whitespace
            if (line.isEmpty()) return;

            if (line.startsWith("customer_id")) {
                // ✅ Header line — ignore silently
                return;
            }

            String[] parts = line.split(",", -1); // -1 includes empty fields
            if (parts.length != 6) {
                out.get(invalidTag).output(line); // ✅ True invalid
                return;
            }

            Customer customer = new Customer(
                    parts[0].trim(),
                    parts[1].trim(),
                    parts[2].trim(),
                    parts[3].trim(),
                    parts[4].trim(),
                    parts[5].trim()
            );
            out.get(validTag).output(customer);
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
