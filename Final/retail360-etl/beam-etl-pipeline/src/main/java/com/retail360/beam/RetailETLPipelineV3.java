package com.retail360.beam;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.io.Serializable;

// ✅ Main Pipeline
public class RetailETLPipelineV3 {


    // ✅ Model class
    static class Customer implements Serializable {
        private String customerId;
        private String firstName;
        private String lastName;
        private String email;
        private String country;
        private String status;

        public Customer(String customerId, String firstName, String lastName, String email, String country, String status) {
            this.customerId = customerId;
            this.firstName = firstName;
            this.lastName = lastName;
            this.email = email;
            this.country = country;
            this.status = status;
        }

        public String getStatus() {
            return status;
        }

        public String format() {
            return String.join(",", customerId, firstName, lastName, email, country, status);
        }
    }

    // ✅ DoFn to parse and branch
    static class ParseFn extends DoFn<String, Customer> {
        private final TupleTag<Customer> validTag;
        private final TupleTag<String> invalidTag;
        private final TupleTag<String> inactiveTag;

        public ParseFn(TupleTag<Customer> validTag, TupleTag<String> invalidTag, TupleTag<String> inactiveTag) {
            this.validTag = validTag;
            this.invalidTag = invalidTag;
            this.inactiveTag = inactiveTag;
        }

        @ProcessElement
        public void processElement(@Element String line, MultiOutputReceiver out) {
            try {
                String[] fields = line.split(",");
                if (fields.length != 6 || line.startsWith("customer_id")) {
                    out.get(invalidTag).output(line);
                    return;
                }

                Customer customer = new Customer(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);
                if ("INACTIVE".equalsIgnoreCase(customer.getStatus())) {
                    out.get(inactiveTag).output(line);
                } else {
                    out.get(validTag).output(customer);
                }
            } catch (Exception e) {
                out.get(invalidTag).output(line);
            }
        }
    }

    // ✅ DoFn to format Customer for output
    static class FormatFn extends DoFn<Customer, String> {
        @ProcessElement
        public void processElement(@Element Customer customer, OutputReceiver<String> out) {
            out.output(customer.format());
        }
    }


    public static final TupleTag<Customer> VALID_CUSTOMERS = new TupleTag<Customer>() {};
    public static final TupleTag<String> INVALID_LINES = new TupleTag<String>() {};
    public static final TupleTag<String> INACTIVE_CUSTOMERS = new TupleTag<String>() {};

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        options.setRunner(DirectRunner.class);  // 🔄 change to DataflowRunner for cloud

        Pipeline pipeline = Pipeline.create(options);

        // GCS paths
        String inputFile = "gs://cloudlab-bucket1/cleaned_customers.csv";
        String validOutput = "gs://cloudlab-bucket1/output/enriched_customers";
        String invalidOutput = "gs://cloudlab-bucket1/output/invalid_rows";
        String inactiveOutput = "gs://cloudlab-bucket1/output/inactive_customers";

        PCollectionTuple results = pipeline
                .apply("ReadFromGCS", TextIO.read().from(inputFile))
                .apply("ParseAndValidate", ParDo.of(
                        new ParseFn(VALID_CUSTOMERS, INVALID_LINES, INACTIVE_CUSTOMERS)
                ).withOutputTags(VALID_CUSTOMERS, TupleTagList.of(INVALID_LINES).and(INACTIVE_CUSTOMERS)));

        results.get(VALID_CUSTOMERS)
                .apply("FormatValid", ParDo.of(new FormatFn()))
                .apply("WriteValid", TextIO.write().to(validOutput).withSuffix(".csv").withNumShards(1));

        results.get(INVALID_LINES)
                .apply("WriteInvalid", TextIO.write().to(invalidOutput).withSuffix(".csv").withNumShards(1));

        results.get(INACTIVE_CUSTOMERS)
                .apply("WriteInactive", TextIO.write().to(inactiveOutput).withSuffix(".csv").withNumShards(1));

        pipeline.run().waitUntilFinish();
    }
}
