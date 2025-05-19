
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.PCollection;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.Schema.Parser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AvroETLPipeline {

        public static void main(String[] args) throws IOException {
        
        Pipeline pipeline = Pipeline.create();

        // 1. Load input Avro schema
        org.apache.avro.Schema inputAvroSchema =
                new Parser().parse(new File("src/main/resources/transaction.avsc"));

        // 2. Load output Avro schema for Parquet
        org.apache.avro.Schema outputAvroSchema =
                new Parser().parse(new File("src/main/resources/account_aggregation.avsc"));

        // 3. Read GenericRecords manually
        List<GenericRecord> recordList = new ArrayList<>();
        try (DataFileReader<GenericRecord> reader =
                     new DataFileReader<>(new SeekableFileInput(
                             new File("src/main/resources/transactions.avro")),
                             new GenericDatumReader<>())) {
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

        // 5. Convert to Beam Rows
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

        // 6. Create input collection
        PCollection<Row> input = pipeline
                .apply("Create Input", org.apache.beam.sdk.transforms.Create.of(rows))
                .setRowSchema(beamSchema);

        // 7. SQL transform
        PCollection<Row> result = input.apply(SqlTransform.query(
                "SELECT accountId, SUM(amount) AS totalAmount " +
                        "FROM PCOLLECTION GROUP BY accountId"));

        // 8. Convert Row to GenericRecord
        PCollection<GenericRecord> outputRecords = result.apply("Convert to GenericRecord", ParDo.of(
                new DoFn<Row, GenericRecord>() {
                    @ProcessElement
                    public void processElement(@Element Row row, OutputReceiver<GenericRecord> out) {
                        GenericRecord record = new GenericData.Record(outputAvroSchema);
                        record.put("accountId", row.getString("accountId"));
                        record.put("totalAmount", row.getDouble("totalAmount"));
                        out.output(record);
                    }
                }));

        // 9. Write to Parquet using Avro schema
        outputRecords.apply("Write Parquet",
        FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(outputAvroSchema))
                .to("output/sql_parquet_output")
                .withSuffix(".parquet")
        );

        pipeline.run().waitUntilFinish();
    }
}
