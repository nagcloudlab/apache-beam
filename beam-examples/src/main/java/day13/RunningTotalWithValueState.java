package day13;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class RunningTotalWithValueState {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // 1. Create TestStream of transactions
        TestStream<Transaction> transactionStream = TestStream.create(SerializableCoder.of(Transaction.class))
                .addElements(
                        TimestampedValue.of(new Transaction("tx1", "A1", 50.0, Instant.parse("2024-01-01T00:00:00Z")), Instant.parse("2024-01-01T00:00:00Z")),
                        TimestampedValue.of(new Transaction("tx2", "A2", 25.0, Instant.parse("2024-01-01T00:00:01Z")), Instant.parse("2024-01-01T00:00:01Z")),
                        TimestampedValue.of(new Transaction("tx3", "A1", 30.0, Instant.parse("2024-01-01T00:00:02Z")), Instant.parse("2024-01-01T00:00:02Z"))
                )
                .advanceWatermarkToInfinity();

        // 2. Apply TestStream and windowing
        PCollection<Transaction> input = pipeline
                .apply(transactionStream)
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));

        // 3. Convert to KV<accountId, Transaction>
        PCollection<KV<String, Transaction>> keyed = input
                .apply("KeyByAccount", ParDo.of(new DoFn<Transaction, KV<String, Transaction>>() {
                    @ProcessElement
                    public void process(@Element Transaction tx, OutputReceiver<KV<String, Transaction>> out) {
                        out.output(KV.of(tx.getAccountId(), tx));
                    }
                }));

        // 4. Apply stateful ParDo with ValueState
        PCollection<KV<String, Double>> result = keyed
                .apply("RunningTotal", ParDo.of(new DoFn<KV<String, Transaction>, KV<String, Double>>() {

                    @StateId("total")
                    private final StateSpec<ValueState<Double>> totalSpec = StateSpecs.value(); // state store

                    @ProcessElement
                    public void process(
                            @Element KV<String, Transaction> element,
                            @StateId("total") ValueState<Double> totalState,
                            OutputReceiver<KV<String, Double>> out) {

                        double current = totalState.read() != null ? totalState.read() : 0.0;
                        double updated = current + element.getValue().getAmount();
                        totalState.write(updated);
                        out.output(KV.of(element.getKey(), updated));
                    }
                }));

        // 5. Print result
        result.apply("PrintOutput", ParDo.of(new DoFn<KV<String, Double>, Void>() {
            @ProcessElement
            public void processElement(@Element KV<String, Double> element) {
                System.out.println("Account: " + element.getKey() + " | Running Total: " + element.getValue());
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
