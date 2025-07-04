package day11;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: Distinct
//   description: Demonstration of Distinct transform usage.
//   multifile: false
//   default_example: false
//   context_line: 46
//   categories:
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - transforms
//     - numbers
//     - distinct

public class DistinctExample {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        // [START main_section]
        // Create a collection with repeating numbers
        PCollection<Integer> input = pipeline.apply(Create.of(1, 1, 2, 2, 3, 4, 4, 5, 5));
        // Apply Distinct transform
        PCollection<Integer> distinctNumbers = input.apply(Distinct.create());
        // [END main_section]
        // Log values
        distinctNumbers.apply(
                ParDo.of(new LogOutput<>("PCollection numbers after Distinct.create transform: ")));
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
