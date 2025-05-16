package day11;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: Combine
//   description: Demonstration of Combine transform usage.
//   multifile: false
//   default_example: false
//   context_line: 47
//   categories:
//     - Core Transforms
//     - Combiners
//   complexity: BASIC
//   tags:
//     - transforms
//     - numbers

public class CombineExample {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        // [START main_section]
        // Sum.ofIntegers() combines the elements in the input PCollection. The
        // resulting PCollection, called sum,
        // contains one value: the sum of all the elements in the input PCollection.
        PCollection<Integer> pc = pipeline.apply(Create.of(1, 2, 3, 4, 5));
        PCollection<Integer> sum = pc.apply(Combine.globally(Sum.ofIntegers()));
        // [END main_section]
        // Log values
        sum.apply(ParDo.of(new LogOutput<>("PCollection numbers after Combine transform: ")));
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
