package com.example.day4;

//import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class BatchProcessingExample {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
//        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("ReadCsv", TextIO.read().from("transactions.csv"))
//                .apply(ParDo.of(new DoFn<String, String>() {
//                    @ProcessElement
//                    public void processElement(ProcessContext c) {
//                        String line = c.element();
//                        // print thread name & id
//                        System.out.println("Thread: " + Thread.currentThread().getName() + ", ID: " + Thread.currentThread().getId());
//                        //System.out.println("Processing line: " + line);
//                        c.output(line);
//                    }
//                }))
                .apply("FilterHeaderRow", Filter.by((String line) -> !line.startsWith("postingDate")))
                .apply("FilterInvalidRecords", Filter.by((String line) -> {
                    String[] parts = line.split(",", -1);
                    if (parts.length != 5) return false;
                    String type = parts[1];
                    String from = parts[2];
                    String to = parts[3];
                    String amount = parts[4];
                    if (type.isEmpty() || amount.isEmpty()) return false;
                    // Type-specific validation
                    if (type.equals("TRANSFER")) {
                        return !from.isEmpty() && !to.isEmpty();
                    } else if (type.equals("DEPOSIT") || type.equals("INTEREST")) {
                        return !to.isEmpty();
                    } else if (type.equals("WITHDRAWAL") || type.equals("FEE")) {
                        return !from.isEmpty();
                    }
                    return false;
                }))
                .apply("LogValidLines", MapElements.into(TypeDescriptor.of(String.class)).via(line -> {
                    //System.out.println("Valid line: " + line);
                    return line;
                }))
                .apply("WriteValidLines", TextIO.write().to("valid_transactions").withSuffix(".csv").withoutSharding());

        pipeline.run().waitUntilFinish();
    }

}
