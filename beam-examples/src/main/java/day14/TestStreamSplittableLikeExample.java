package day14;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

import java.io.Serializable;

// Simple Transaction POJO
class Transaction implements Serializable {
    public String txId;
    public String accountId;
    public double amount;
    public String timestamp;

    public Transaction(String txId, String accountId, double amount, String timestamp) {
        this.txId = txId;
        this.accountId = accountId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Transaction(" + txId + ", " + accountId + ", " + amount + ", " + timestamp + ")";
    }
}

// Simulates splitting logic like SplittableDoFn
class SplitEvenOddDoFn extends DoFn<Transaction, String> {
    @ProcessElement
    public void processElement(@Element Transaction tx, OutputReceiver<String> out) {
        if ((int) tx.amount % 2 == 0) {
            out.output("EvenTx: " + tx);
        } else {
            out.output("OddTx: " + tx);
        }
    }
}

public class TestStreamSplittableLikeExample {

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

        // Simulated transactions
        Transaction t1 = new Transaction("tx1", "A1", 50.0, "2024-01-01T00:00:00Z");
        Transaction t2 = new Transaction("tx2", "A2", 35.0, "2024-01-01T00:00:01Z");
        Transaction t3 = new Transaction("tx3", "A1", 60.0, "2024-01-01T00:00:02Z");

        // Simulate streaming input using TestStream
        TestStream<Transaction> testStream = TestStream.create(SerializableCoder.of(Transaction.class))
                .addElements(t1)
                .advanceWatermarkTo(Instant.parse("2024-01-01T00:00:01Z"))
                .addElements(t2)
                .advanceWatermarkTo(Instant.parse("2024-01-01T00:00:02Z"))
                .addElements(t3)
                .advanceWatermarkToInfinity();

        // Apply stream to pipeline
        PCollection<Transaction> txStream = pipeline.apply(testStream);

        // Apply simulated "splitting" logic
        txStream
                .apply("SplitEvenOdd", ParDo.of(new SplitEvenOddDoFn()))
                .apply("Print", ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(@Element String line) {
                        System.out.println(line);
                    }
                }));

        pipeline.run().waitUntilFinish();
    }
}
