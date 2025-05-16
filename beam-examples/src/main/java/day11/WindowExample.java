package day11;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class WindowExample {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        // [START main_section]
        // Create some input data with timestamps
        List<String> inputData = Arrays.asList("foo", "bar", "foo", "foo");
        List<Long> timestamps =
                Arrays.asList(
                        Duration.standardSeconds(15).getMillis(),
                        Duration.standardSeconds(30).getMillis(),
                        Duration.standardSeconds(45).getMillis(),
                        Duration.standardSeconds(90).getMillis());

        // Create a PCollection from the input data with timestamps
        PCollection<String> items = pipeline.apply(Create.timestamped(inputData, timestamps));

        // Create a windowed PCollection
        PCollection<String> windowedItems =
                items.apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));

        PCollection<KV<String, Long>> windowedCounts = windowedItems.apply(Count.perElement());
        // [END main_section]
        windowedCounts.apply(ParDo.of(new LogOutput<>("PCollection elements after Count transform: ")));
        pipeline.run();
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private static final Logger LOG = LoggerFactory.getLogger(LogOutput.class);
        private final String prefix;

        public LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info(prefix + c.element());
            c.output(c.element());
        }
    }
}
