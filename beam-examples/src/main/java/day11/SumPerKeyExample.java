package day11;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: SumPerKey
//   description: Demonstration of Sum.integersPerKey transform usage.
//   multifile: false
//   default_example: false
//   context_line: 47
//   categories:
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - transforms
//     - numbers
//     - pairs

public class SumPerKeyExample {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        // [START main_section]
        PCollection<KV<String, Integer>> input =
                pipeline.apply(
                        Create.of(KV.of("a", 1), KV.of("a", 2), KV.of("b", 3), KV.of("b", 4), KV.of("b", 5)));
        PCollection<KV<String, Integer>> sumPerKey = input.apply(Sum.integersPerKey());
        // [END main_section]
        // Log values
        sumPerKey.apply(ParDo.of(new LogOutput<>("PCollection numbers after Sum transform: ")));
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
