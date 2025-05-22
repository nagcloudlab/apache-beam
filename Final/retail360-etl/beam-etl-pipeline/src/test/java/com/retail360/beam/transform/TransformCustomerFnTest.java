package com.retail360.beam.transform;

import com.retail360.beam.model.Customer;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class TransformCustomerFnTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testTransformCustomer() {
        Customer c = new Customer("1001", "Alice", "Smith", "alice@example.com", "USA", "ACTIVE");

        PCollection<Customer> input = pipeline.apply("Create", org.apache.beam.sdk.transforms.Create.of(c));
        PCollection<String> output = input.apply("Transform", ParDo.of(new TransformCustomerFn()));

        String expected = "1001,Alice Smith,alice@example.com,USA,ACTIVE,ENRICHED";
        PAssert.that(output).containsInAnyOrder(expected);
        pipeline.run().waitUntilFinish();
    }
}
