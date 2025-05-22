package com.retail360.beam;


import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class RetailETLPipelineV4 {

    // Define TupleTags
    public static final TupleTag<Customer> VALID_CUSTOMERS = new TupleTag<Customer>() {};
    public static final TupleTag<String> INVALID_LINES = new TupleTag<String>() {};
    public static final TupleTag<String> INACTIVE_CUSTOMERS = new TupleTag<String>() {};

    // Model class
    public static class Customer implements Serializable {
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

    // ParseFn with tagging
    static class ParseFn extends DoFn<String, Customer> {

        private static final Logger logger = LoggerFactory.getLogger(ParseFn.class);

        @ProcessElement
        public void processElement(@Element String line, MultiOutputReceiver out) {
            try {
                String[] fields = line.split(",");
                if (fields.length != 6) {
                    out.get(RetailETLPipelineV4.INVALID_LINES).output(line);
                    return;
                }

                Customer c = new Customer(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);

                if (c.getStatus().equalsIgnoreCase("INACTIVE")) {
                    out.get(RetailETLPipelineV4.INACTIVE_CUSTOMERS).output(line);
                } else {
                    out.get(RetailETLPipelineV4.VALID_CUSTOMERS).output(c);
                }
            } catch (Exception e) {
                logger.error("❌ Failed to parse line: " + line, e);
                out.get(RetailETLPipelineV4.INVALID_LINES).output(line);
            }
        }
    }

    // FormatFn for valid customer
    public static class FormatFn extends DoFn<Customer, String> {
        @ProcessElement
        public void processElement(@Element Customer customer, OutputReceiver<String> out) {
            out.output(customer.format());
        }
    }

    // Pipeline options interface for Dataflow
    public interface RetailPipelineOptions extends PipelineOptions {
        @Description("Path of the input file")
        @Default.String("gs://cloudlab-bucket1/cleaned_customers.csv")
        String getInputFile();
        void setInputFile(String value);

        @Description("Output path for valid customers")
        @Default.String("gs://cloudlab-bucket1/output/enriched_customers")
        String getValidOutput();
        void setValidOutput(String value);

        @Description("Output path for invalid rows")
        @Default.String("gs://cloudlab-bucket1/output/invalid_rows")
        String getInvalidOutput();
        void setInvalidOutput(String value);

        @Description("Output path for inactive customers")
        @Default.String("gs://cloudlab-bucket1/output/inactive_customers")
        String getInactiveOutput();
        void setInactiveOutput(String value);
    }

    public static void main(String[] args) {
        RetailPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(RetailPipelineOptions.class);

        options.setRunner(DataflowRunner.class);
        options.setJobName("retail-etl-dataflow-job");

        Pipeline pipeline = Pipeline.create(options);

        PCollectionTuple results = pipeline
                .apply("ReadCSV", TextIO.read().from(options.getInputFile()))
                .apply("ParseAndBranch", ParDo.of(
                                new ParseFn())
                        .withOutputTags(VALID_CUSTOMERS, TupleTagList.of(INVALID_LINES).and(INACTIVE_CUSTOMERS)));

        results.get(VALID_CUSTOMERS)
                .apply("FormatValid", ParDo.of(new FormatFn()))
                .apply("WriteValid", TextIO.write().to(options.getValidOutput()).withSuffix(".csv").withNumShards(1));

        results.get(INVALID_LINES)
                .apply("WriteInvalid", TextIO.write().to(options.getInvalidOutput()).withSuffix(".csv").withNumShards(1));

        results.get(INACTIVE_CUSTOMERS)
                .apply("WriteInactive", TextIO.write().to(options.getInactiveOutput()).withSuffix(".csv").withNumShards(1));

        pipeline.run();
    }
}
