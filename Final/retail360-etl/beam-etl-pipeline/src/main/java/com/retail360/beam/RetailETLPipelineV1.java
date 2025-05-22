package com.retail360.beam;

import com.retail360.beam.transform.FormatFn;
import com.retail360.beam.transform.ParseFn;
import com.retail360.beam.transform.TransformFn;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

public class RetailETLPipelineV1 {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        options.setRunner(DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("ReadCleanedCSV", TextIO.read().from("src/main/resources/cleaned_customers.csv"))
                .apply("ParseCSV", ParDo.of(new ParseFn()))
                .apply("TransformData", ParDo.of(new TransformFn()))
                .apply("FormatCSV", ParDo.of(new FormatFn()))
                .apply("WriteCSV", TextIO.write()
                        .to("output/enriched_customers")
                        .withHeader("customer_id,full_name,email,country,status")
                        .withSuffix(".csv")
                        .withoutSharding());

        pipeline.run().waitUntilFinish();
    }
}
