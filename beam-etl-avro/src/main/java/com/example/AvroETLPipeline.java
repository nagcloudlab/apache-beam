package com.example;

import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import java.io.File;
import java.nio.file.Files;

public class AvroETLPipeline {
    public static void main(String[] args) throws Exception {

        // Load Avro schema
        String avroSchemaString = Files.readString(new File("customer.avsc").toPath());
        org.apache.avro.Schema avroSchema = new Parser().parse(avroSchemaString);

        // Define Beam schema manually
        Schema beamSchema = Schema.builder()
                .addStringField("id")
                .addStringField("name")
                .addNullableField("email", Schema.FieldType.STRING)
                .addNullableField("age", Schema.FieldType.INT32)
                .build();

        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());

        // Read input Avro file as GenericRecord
        PCollection<GenericRecord> input = pipeline.apply(
                AvroIO.readGenericRecords(avroSchema).from("input/customers.avro"));

        // Schema Aware Beam pipelines
        // Convert GenericRecord to Beam Row manually
        PCollection<Row> rows = input
                .apply("Convert to Row", ParDo.of(new DoFn<GenericRecord, Row>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        GenericRecord record = c.element();
                        Row row = Row.withSchema(beamSchema)
                                .addValue(record.get("id").toString())
                                .addValue(record.get("name").toString())
                                .addValue(record.get("email") != null ? record.get("email").toString() : null)
                                .addValue(record.get("age") != null ? (Integer) record.get("age") : null)
                                .build();
                        c.output(row);
                    }
                })).setRowSchema(beamSchema);



        // Filter: only keep rows with non-null email
        PCollection<Row> filtered = rows.apply("Filter null emails", ParDo.of(new DoFn<Row, Row>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Row row = c.element();
                if (row.getString("email") != null) {
                    c.output(row);
                }
            }
        })).setRowSchema(beamSchema);

        // Print filtered output
        filtered.apply("Print", ParDo.of(new DoFn<Row, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println("✔️ Valid row: " + c.element());
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
