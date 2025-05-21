package day14;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.*;

public class SideInputCombinedExample {

    // Transaction class
    public static class Transaction implements Serializable {
        public String transactionId;
        public String accountId;
        public String currency;
        public double amount;
        public Instant timestamp;

        public Transaction(String txId, String accId, String currency, double amt, Instant ts) {
            this.transactionId = txId;
            this.accountId = accId;
            this.currency = currency;
            this.amount = amt;
            this.timestamp = ts;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Transaction that = (Transaction) o;
            return Double.compare(amount, that.amount) == 0 && Objects.equals(transactionId, that.transactionId) && Objects.equals(accountId, that.accountId) && Objects.equals(currency, that.currency) && Objects.equals(timestamp, that.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(transactionId, accountId, currency, amount, timestamp);
        }

        @Override
        public String toString() {
            return String.format("Transaction(%s, %s, %s, %.2f, %s)",
                    transactionId, accountId, currency, amount, timestamp);
        }
    }

    public static void main(String[] args) {

        Pipeline p = Pipeline.create();

        // === SIDE INPUTS ===

        // 1. Static single value side input: min thresholds
        Map<String, Double> minThresholds = Map.of("USD", 50.0, "EUR", 60.0);
        final PCollectionView<Map<String, Double>> minThresholdView =
                p.apply("MinThresholds", Create.of(minThresholds))
                        .apply(View.asMap());

        // 2. Static Map side input: accountId to user name
        Map<String, String> userLookup = Map.of("A1", "Alice", "A2", "Bob", "A3", "Carol");
        final PCollectionView<Map<String, String>> userMapView =
                p.apply("UserMap", Create.of(userLookup))
                        .apply(View.asMap());

        // === INPUT TEST STREAM ===

        TestStream<Transaction> txStream = TestStream.create(SerializableCoder.of(Transaction.class))
                .addElements(
                        TimestampedValue.of(new Transaction("tx1", "A1", "USD", 40.0, new Instant("2024-01-01T00:00:00Z")), new Instant("2024-01-01T00:00:00Z")),
                        TimestampedValue.of(new Transaction("tx2", "A2", "USD", 70.0, new Instant("2024-01-01T00:00:01Z")), new Instant("2024-01-01T00:00:01Z")),
                        TimestampedValue.of(new Transaction("tx3", "A3", "EUR", 80.0, new Instant("2024-01-01T00:00:02Z")), new Instant("2024-01-01T00:00:02Z"))
                )
                .advanceWatermarkToInfinity();

        PCollection<Transaction> transactions = p.apply("Tx Stream", txStream);

        // === MAIN LOGIC ===

        transactions
                .apply("Enrich & Filter with SideInputs", ParDo.of(new DoFn<Transaction, String>() {
                    @ProcessElement
                    public void processElement(@Element Transaction tx,
                                               OutputReceiver<String> out,
                                               ProcessContext c) {

                        Map<String, Double> thresholds = c.sideInput(minThresholdView);
                        Map<String, String> userMap = c.sideInput(userMapView);

                        double min = thresholds.getOrDefault(tx.currency, 0.0);

                        if (tx.amount >= min) {
                            String user = userMap.getOrDefault(tx.accountId, "Unknown");
                            out.output(String.format("User: %s | Tx: %s | Amount: %.2f %s",
                                    user, tx.transactionId, tx.amount, tx.currency));
                        }
                    }
                }).withSideInputs(minThresholdView, userMapView))
                .apply("Print", ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(@Element String line) {
                        System.out.println(line);
                    }
                }));

        p.run().waitUntilFinish();
    }
}

