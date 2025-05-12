package com.example.transforms;


import com.example.model.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class JsonToTransactionTransform extends DoFn<KV<String, String>, Transaction> {

    private final static ObjectMapper objectMapper = new ObjectMapper();

    public static JsonToTransactionTransform of() {
        return new JsonToTransactionTransform();
    }
    @DoFn.ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        String jsonString = c.element().getValue();
        Transaction transaction = objectMapper.readValue(jsonString, Transaction.class);
        //System.out.println("Received: " + transaction);
        c.output(transaction);
    }


    public static Transaction parseJson(String jsonString) {
        try {
            return objectMapper.readValue(jsonString, Transaction.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
