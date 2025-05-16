package com.example;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.VoidSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class KafkaAvroFilterPipeline {
    public static void main(String[] args) throws Exception {

        String bootstrapServers = "localhost:9092";
        String inputTopic = "transfers";
        String outputTopic = "high-transfers";
        String schemaPath = "transfer_event.avsc";

        Schema avroSchema = new Schema.Parser().parse(Files.readString(Paths.get(schemaPath)));

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        options.setRunner(org.apache.beam.runners.direct.DirectRunner.class);
        options.setJobName("KafkaAvroFilterPipeline");
        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);

        pipeline
                .apply("ReadFromKafka",
                        org.apache.beam.sdk.io.kafka.KafkaIO.<String, byte[]>read()
                                .withBootstrapServers(bootstrapServers)
                                .withTopic(inputTopic)
                                .withKeyDeserializer(org.apache.kafka.common.serialization.StringDeserializer.class)
                                .withValueDeserializer(org.apache.kafka.common.serialization.ByteArrayDeserializer.class)
                                .withoutMetadata())
                .apply("DeserializeAvro", ParDo.of(new DoFn<KV<String, byte[]>, GenericRecord>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws IOException {
                        byte[] bytes = c.element().getValue();
                        DatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
                        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
                        GenericRecord record = reader.read(null, decoder);
                        c.output(record);
                    }
                }))
                .setCoder(org.apache.beam.sdk.extensions.avro.coders.AvroCoder.of(avroSchema))
                .apply("FilterAmountGT100", Filter.by((GenericRecord record) -> {
                    Double amount = (Double) record.get("amount");
                    return amount != null && amount > 100.0;
                }))
                .apply("SerializeAvro", MapElements.via(new SimpleFunction<GenericRecord, KV<Void, byte[]>>() {
                    @Override
                    public KV<Void, byte[]> apply(GenericRecord record) {
                        try {
                            ByteArrayOutputStream out = new ByteArrayOutputStream();
                            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroSchema);
                            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                            writer.write(record, encoder);
                            encoder.flush();
                            return KV.of(null, out.toByteArray());
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to serialize Avro record", e);
                        }
                    }
                }))
                .apply("WriteToKafka", KafkaIO.<Void, byte[]>write()
                        .withBootstrapServers(bootstrapServers)
                        .withTopic(outputTopic)
                        .withKeySerializer(VoidSerializer.class)
                        .withValueSerializer(ByteArraySerializer.class)
                );

        pipeline.run().waitUntilFinish();

    }
}
