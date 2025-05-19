package day12;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;


// data with schema
// avro
// parquet
// SQL tables
// i.e structured data



public class RowTransactionPipeline {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        Schema schema = Schema.builder()
                .addStringField("transactionId")
                .addStringField("accountId")
                .addDoubleField("amount")
                .addInt64Field("timestamp")
                .build();

        Row r1 = Row.withSchema(schema)
                .addValues("tx001", "acc101", 1500.0, 1716000000000L).build();

        Row r2 = Row.withSchema(schema)
                .addValues("tx002", "acc102", 500.0, 1716003600000L).build();

        PCollection<Row> txns = pipeline
                .apply(Create.of(r1, r2))
                .setRowSchema(schema);

        PCollection<Row> highValue = txns.apply(Filter.by(row -> row.getDouble("amount") > 1000));

        // Beam SQL
//        txns.apply(SqlTransform.query(
//                "SELECT accountId, SUM(amount) AS total " +
//                        "FROM PCOLLECTION WHERE amount > 1000 GROUP BY accountId"
//        ));

        highValue.apply(ParDo.of(new DoFn<Row, Void>() {
            @ProcessElement
            public void processElement(@Element Row row, OutputReceiver<Void> out) {
                System.out.println("High: " + row.getString("accountId") + " - " + row.getDouble("amount"));
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
