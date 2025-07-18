package day11;


import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.InferableFunction;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: FlatMapElements
//   description: Demonstration of FlatMapElements transform usage.
//   multifile: false
//   default_example: false
//   context_line: 50
//   categories:
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - transforms
//     - pairs
//     - strings
//     - map

public class FlatMapElementsExample {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        // [START main_section]
        PCollection<String> lines =
                pipeline.apply(Create.of("the quick brown fox", "jumps over the lazy", "dog"));
        // Use FlatMapElements to split the lines in PCollection<String> into individual
        // words.
        PCollection<String> words =
                lines.apply(
                        FlatMapElements.via(
                                new InferableFunction<String, List<String>>() {
                                    @Override
                                    public List<String> apply(String line) {
                                        return Arrays.asList(line.split(" "));
                                    }
                                }));
        // [END main_section]
        words.apply(ParDo.of(new LogOutput<>("PCollection elements after the transform: ")));
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
