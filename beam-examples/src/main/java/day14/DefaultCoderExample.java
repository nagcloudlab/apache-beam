package day14;


import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.Arrays;


public class DefaultCoderExample {

    @DefaultCoder(AvroCoder.class)
    public static class Transaction {
        public String txId;
        public String accountId;
        public double amount;

        // Required for AvroCoder
        public Transaction() {}

        public Transaction(String txId, String accountId, double amount) {
            this.txId = txId;
            this.accountId = accountId;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return String.format("Transaction(%s, %s, %.2f)", txId, accountId, amount);
        }
    }

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // 1. Create a list of transactions
        PCollection<Transaction> txns = pipeline.apply("Create Transactions", Create.of(
                new Transaction("tx1", "A1", 100.0),
                new Transaction("tx2", "A2", 200.0),
                new Transaction("tx3", "A1", 150.0),
                new Transaction("tx4", "A2", 50.0)
        ));

        // 2. Key by accountId
        PCollection<KV<String, Double>> accountTotals = txns
                .apply("Map to KV", MapElements.into(TypeDescriptors.kvs(
                                TypeDescriptors.strings(), TypeDescriptors.doubles()))
                        .via(tx -> KV.of(tx.accountId, tx.amount)))
                .apply("Sum by Account", Sum.doublesPerKey());

        // 3. Print results
        accountTotals.apply("Print", ParDo.of(new DoFn<KV<String, Double>, Void>() {
            @ProcessElement
            public void processElement(@Element KV<String, Double> element) {
                System.out.println("Account: " + element.getKey() + ", Total: " + element.getValue());
            }
        }));

        pipeline.run().waitUntilFinish();
    }

}
