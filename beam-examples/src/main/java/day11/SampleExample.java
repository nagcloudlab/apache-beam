package day11;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: Sample
//   description: Demonstration of Sample transform usage.
//   multifile: false
//   default_example: false
//   context_line: 47
//   categories:
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - transforms
//     - pairs
//     - group

public class SampleExample {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        // [START main_section]
        // Create pairs
        PCollection<KV<String, String>> pairs =
                pipeline.apply(
                        Create.of(
                                KV.of("fall", "apple"),
                                KV.of("spring", "strawberry"),
                                KV.of("winter", "orange"),
                                KV.of("summer", "peach"),
                                KV.of("spring", "cherry"),
                                KV.of("fall", "pear")));
        // We use Sample.fixedSizePerKey() to get fixed-size random samples for each
        // unique key in a PCollection of key-values.
        PCollection<KV<String, Iterable<String>>> result = pairs.apply(Sample.fixedSizePerKey(2));
        // [END main_section]
        result.apply(
                ParDo.of(new LogOutput<>("PCollection pairs after Sample.fixedSizePerKey transform: ")));
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
