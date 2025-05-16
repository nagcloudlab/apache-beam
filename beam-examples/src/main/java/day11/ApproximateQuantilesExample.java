package day11;


import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ApproximateQuantiles;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: ApproximateQuantiles
//   description: Demonstration of ApproximateQuantiles transform usage.
//   multifile: false
//   default_example: false
//   context_line: 46
//   categories:
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - transforms
//     - numbers

public class ApproximateQuantilesExample {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        // [START main_section]
        // Create a collection with numbers
        PCollection<Integer> input = pipeline.apply(Create.of(1, 1, 2, 2, 3, 4, 4, 5, 5));
        // This will produce a collection containing 5 values: the minimum value,
        // Quartile 1 value, Quartile 2 value (median), Quartile 3 value, and the
        // maximum value.
        PCollection<List<Integer>> result = input.apply(ApproximateQuantiles.globally(5));
        // [END main_section]
        result.apply(
                ParDo.of(
                        new LogOutput<>(
                                "PCollection numbers after ApproximateQuantiles.globally transform: ")));
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
