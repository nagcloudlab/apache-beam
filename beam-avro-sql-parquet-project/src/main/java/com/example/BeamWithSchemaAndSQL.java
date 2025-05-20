package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BeamWithSchemaAndSQL {

    public static void main(String[] args) throws IOException {

        Pipeline pipeline = Pipeline.create();

        // 1. Load input Avro schema
        org.apache.avro.Schema inputAvroSchema =
                new Parser().parse(new File("src/main/resources/transaction.avsc"));

        // 2. Load output Avro schema for Parquet
        org.apache.avro.Schema outputAvroSchema =
                new Parser().parse(new File("src/main/resources/account_aggregation.avsc"));

        // 3. Read GenericRecords using inputAvroSchema
        List<GenericRecord> recordList = new ArrayList<>();
        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(
                new SeekableFileInput(new File("src/main/resources/transactions.avro")),
                new GenericDatumReader<>(inputAvroSchema))) {
            while (reader.hasNext()) {
                recordList.add(reader.next());
            }
        }

        // 4. Define Beam Schema
        Schema beamSchema = Schema.builder()
                .addStringField("transactionId")
                .addStringField("accountId")
                .addDoubleField("amount")
                .addInt64Field("timestamp")
                .build();

        // 5. Convert GenericRecords to Beam Rows
        List<Row> rows = new ArrayList<>();
        for (GenericRecord gr : recordList) {
            rows.add(Row.withSchema(beamSchema)
                    .addValues(
                            gr.get("transactionId").toString(),
                            gr.get("accountId").toString(),
                            (double) gr.get("amount"),
                            (long) gr.get("timestamp"))
                    .build());
        }

        // 6. Create input PCollection<Row> like table
        PCollection<Row> input = pipeline
                .apply("Create Input", org.apache.beam.sdk.transforms.Create.of(rows))
                .setRowSchema(beamSchema);

        // 7. SQL Transformation
        PCollection<Row> result = input.apply(SqlTransform.query(
                "SELECT accountId, SUM(amount) AS totalAmount " +
                        "FROM PCOLLECTION GROUP BY accountId"));

        // 8. Convert result Rows to GenericRecord
        PCollection<GenericRecord> outputRecords = result
                .apply("Convert to GenericRecord", ParDo.of(new DoFn<Row, GenericRecord>() {
                    @ProcessElement
                    public void processElement(@Element Row row, OutputReceiver<GenericRecord> out) {
                        GenericRecord record = new GenericData.Record(outputAvroSchema);
                        record.put("accountId", row.getString("accountId"));
                        record.put("totalAmount", row.getDouble("totalAmount"));
                        out.output(record);
                    }
                }))
                .setCoder(AvroCoder.of(GenericRecord.class, outputAvroSchema)); // Explicit coder

        // 9. Write to Parquet
        outputRecords.apply("Write Parquet",
                FileIO.<GenericRecord>write()
                        .via(ParquetIO.sink(outputAvroSchema))
                        .to("output/sql_parquet_output")
                        .withSuffix(".parquet"));

        pipeline.run().waitUntilFinish();
    }
}
