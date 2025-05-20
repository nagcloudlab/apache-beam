package day13;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.testing.TestStream;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class BagStateExample {

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();
        pipeline.getCoderRegistry().registerCoderForClass(String.class, StringUtf8Coder.of());
        pipeline.getCoderRegistry().registerCoderForClass(Transaction.class, SerializableCoder.of(Transaction.class));

        TestStream<Transaction> testStream = TestStream.create(SerializableCoder.of(Transaction.class))
                .addElements(
                        Transaction.of("tx1", "A1", 50.0, Instant.parse("2024-01-01T00:00:00Z")),
                        Transaction.of("tx2", "A1", 70.0, Instant.parse("2024-01-01T00:00:01Z")),
                        Transaction.of("tx3", "A2", 90.0, Instant.parse("2024-01-01T00:00:02Z")),
                        Transaction.of("tx4", "A2", 60.0, Instant.parse("2024-01-01T00:00:03Z"))
                )
                .advanceWatermarkToInfinity();

        PCollection<KV<String, Transaction>> keyed = pipeline
                .apply(testStream)
                .apply("Assign Keys", ParDo.of(new DoFn<Transaction, KV<String, Transaction>>() {
                    @ProcessElement
                    public void processElement(@Element Transaction tx, OutputReceiver<KV<String, Transaction>> out) {
                        out.output(KV.of(tx.getAccountId(), tx));
                    }
                }))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Transaction.class)));

        PCollection<String> result = keyed
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply("BagState Usage", ParDo.of(new DoFn<KV<String, Transaction>, String>() {

                    @StateId("recentTxns")
                    private final StateSpec<BagState<Transaction>> recentTxns = StateSpecs.bag();

                    @ProcessElement
                    public void process(@Element KV<String, Transaction> element,
                                        @StateId("recentTxns") BagState<Transaction> state,
                                        OutputReceiver<String> out) {
                        state.add(element.getValue());

                        StringBuilder sb = new StringBuilder("Recent transactions for " + element.getKey() + ":\n");
                        for (Transaction t : state.read()) {
                            sb.append("  - ").append(t).append("\n");
                        }
                        out.output(sb.toString());
                    }
                }));

        result.apply("Print", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void process(@Element String line) {
                System.out.println(line);
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}