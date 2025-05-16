package day11;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ToStringExample {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        // [START main_section]
        // Create key-value pairs
        PCollection<KV<String, String>> pairs =
                pipeline.apply(
                        Create.of(
                                KV.of("fall", "apple"),
                                KV.of("spring", "strawberry"),
                                KV.of("winter", "orange"),
                                KV.of("summer", "peach"),
                                KV.of("spring", "cherry"),
                                KV.of("fall", "pear")));
        // Use ToString on key-value pairs
        PCollection<String> result = pairs.apply(ToString.kvs());
        // [END main_section]
        result.apply(
                ParDo.of(new LogOutput<>("PCollection key-value pairs after ToString transform: ")));
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
