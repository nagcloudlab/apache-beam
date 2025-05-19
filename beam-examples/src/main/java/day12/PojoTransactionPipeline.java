package day12;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;

// source
// strings
// numbers
// csv
// json
// logs
// i.e any unstrcutured

public class PojoTransactionPipeline {
    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

        PCollection<Transaction> txns = pipeline.apply(Create.of(
                new Transaction("tx001", "acc101", 1500.0, 1716000000000L),
                new Transaction("tx002", "acc102", 500.0, 1716003600000L)
        ));

        PCollection<Transaction> highValue = txns.apply(Filter.by(tx -> tx.amount > 1000));
        // Aggregation
        // GroupByKey

        highValue.apply(ParDo.of(new DoFn<Transaction, Void>() {
            @ProcessElement
            public void processElement(@Element Transaction txn, OutputReceiver<Void> out) {
                System.out.println("High: " + txn.accountId + " - " + txn.amount);
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}

