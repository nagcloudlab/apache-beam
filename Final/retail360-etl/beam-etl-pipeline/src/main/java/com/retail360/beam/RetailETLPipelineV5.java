package com.retail360.beam;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.ArrayList;
import java.util.List;

public class RetailETLPipelineV5 {

    public interface RetailOptions extends org.apache.beam.sdk.options.PipelineOptions {
        @Description("Path to the input file")
        @Validation.Required
        String getInputFile();
        void setInputFile(String value);

        @Description("GCS path for invalid records")
        @Validation.Required
        String getInvalidOutput();
        void setInvalidOutput(String value);

        @Description("GCS path for inactive customers")
        @Validation.Required
        String getInactiveOutput();
        void setInactiveOutput(String value);

        @Description("BigQuery table: project:dataset.table")
        @Validation.Required
        String getBigQueryTable();
        void setBigQueryTable(String value);
    }

    public static void main(String[] args) {
        RetailOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(RetailOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForClass(Customer.class, AvroCoder.of(Customer.class));


        PCollection<String> lines = pipeline.apply("Read CSV", TextIO.read().from(options.getInputFile()));

        PCollectionTuple results = lines.apply("Parse and Branch", ParDo.of(new ParseAndBranchFn())
                .withOutputTags(BranchTags.VALID, TupleTagList.of(BranchTags.INVALID).and(BranchTags.INACTIVE)));

        // ✅ Write valid rows to BigQuery
        results.get(BranchTags.VALID)
                .apply("ToTableRow", MapElements.into(TypeDescriptor.of(TableRow.class))
                        .via((Customer customer) -> new TableRow()
                                .set("customer_id", customer.customerId)
                                .set("name", customer.name)
                                .set("email", customer.email)
                                .set("is_active", customer.isActive)))
                .apply("Write to BigQuery", BigQueryIO.writeTableRows()
                        .to(options.getBigQueryTable())
                        .withSchema(getBQSchema())
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        // ❌ Write invalid rows to GCS
        results.get(BranchTags.INVALID)
                .apply("Write Invalid", TextIO.write().to(options.getInvalidOutput()).withSuffix(".csv").withNumShards(1));

        // ⚪ Write inactive customers to GCS
        results.get(BranchTags.INACTIVE)
                .apply("Format Inactive", MapElements.into(TypeDescriptors.strings()).via(Customer::toString))
                .apply("Write Inactive", TextIO.write().to(options.getInactiveOutput()).withSuffix(".csv").withNumShards(1));

        pipeline.run();
    }

    // Schema for BigQuery table
    private static TableSchema getBQSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("customer_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("email").setType("STRING"));
        fields.add(new TableFieldSchema().setName("is_active").setType("BOOLEAN"));
        return new TableSchema().setFields(fields);
    }

    // POJO class for customer
    @DefaultCoder(AvroCoder.class)
    static class Customer {
        String customerId;
        String name;
        String email;
        boolean isActive;

        public Customer() {}

        Customer(String customerId, String name, String email, boolean isActive) {
            this.customerId = customerId;
            this.name = name;
            this.email = email;
            this.isActive = isActive;
        }

        @Override
        public String toString() {
            return String.join(",", customerId, name, email, String.valueOf(isActive));
        }
    }

    // Tags for branching
    static class BranchTags {
        static final TupleTag<Customer> VALID = new TupleTag<Customer>() {};
        static final TupleTag<String> INVALID = new TupleTag<String>() {};
        static final TupleTag<Customer> INACTIVE = new TupleTag<Customer>() {};
    }

    // Branching DoFn
    static class ParseAndBranchFn extends DoFn<String, Customer> {
        @ProcessElement
        public void processElement(@Element String line, MultiOutputReceiver out) {
            String[] parts = line.split(",");
            if (parts.length != 4) {
                out.get(BranchTags.INVALID).output(line);
                return;
            }

            String id = parts[0].trim();
            String name = parts[1].trim();
            String email = parts[2].trim();
            boolean isActive = Boolean.parseBoolean(parts[3].trim());

            if (id.isEmpty() || name.isEmpty() || email.isEmpty()) {
                out.get(BranchTags.INVALID).output(line);
            } else if (!isActive) {
                out.get(BranchTags.INACTIVE).output(new Customer(id, name, email, false));
            } else {
                out.get(BranchTags.VALID).output(new Customer(id, name, email, true));
            }
        }
    }
}
