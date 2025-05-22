package com.retail360.beam.transform;

import com.retail360.beam.model.Customer;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class FilterActiveFnTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testFilterActiveCustomers() {
        Customer c1 = new Customer("1", "Alice", "Smith", "alice@example.com", "USA", "ACTIVE");
        Customer c2 = new Customer("2", "Bob", "Brown", "bob@example.com", "UK", "INACTIVE");
        Customer c3 = new Customer("3", "Charlie", "Lee", "charlie@lee.io", "Canada", "ACTIVE");

        PCollection<Customer> input = pipeline.apply("CreateCustomers", org.apache.beam.sdk.transforms.Create.of(c1, c2, c3));
        PCollection<Customer> output = input.apply("FilterActive", ParDo.of(new FilterActiveFn()));

        PAssert.that(output).containsInAnyOrder(c1, c3);
        pipeline.run().waitUntilFinish();
    }
}
