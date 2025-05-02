package com.example.transforms.element_wise;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import java.util.Arrays;

public class ViewAsSingletonExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Main data: product prices
        PCollection<Double> prices = pipeline.apply("CreatePrices",
                Create.of(100.0, 200.0, 300.0));

        // Side input: tax rate (assume single value)
        PCollectionView<Double> taxRateView = pipeline
                .apply("CreateTaxRate", Create.of(0.18))
                .apply("ToSingletonView", View.asSingleton());

        // Apply tax using side input
        PCollection<Double> finalPrices = prices.apply("ApplyTax", ParDo.of(new DoFn<Double, Double>() {
            @ProcessElement
            public void processElement(@Element Double price, ProcessContext ctx) {
                Double taxRate = ctx.sideInput(taxRateView);
                Double finalPrice = price + (price * taxRate);
                System.out.println("Final Price: " + finalPrice);
                ctx.output(finalPrice);
            }
        }).withSideInputs(taxRateView));

        pipeline.run().waitUntilFinish();
    }
}
