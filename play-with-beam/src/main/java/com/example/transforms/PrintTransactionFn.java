package com.example.transforms;


import com.example.model.Transaction;
import org.apache.beam.sdk.transforms.DoFn;

public class PrintTransactionFn extends DoFn<Transaction, Void> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        Transaction transaction = c.element();
        System.out.println("Received Transaction: " + transaction);
    }
}
