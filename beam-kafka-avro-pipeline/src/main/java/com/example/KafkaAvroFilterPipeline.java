package com.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class KafkaAvroFilterPipeline {
    public static void main(String[] args) throws Exception {
        String bootstrapServers = "localhost:9092";
        String inputTopic = "transfers";
        String outputTopic = "high_value_transfers";
        String schemaPath = "transfer_event.avsc";

        Schema avroSchema = new Schema.Parser().parse(Files.readString(Paths.get(schemaPath)));

        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create());

        pipeline
                .apply("ReadFromKafka", KafkaIO.<String, byte[]>read()
                        .withBootstrapServers(bootstrapServers)
                        .withTopic(inputTopic)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(ByteArrayDeserializer.class)
                        .withoutMetadata()
                )
                .apply("DeserializeAvro", MapElements.via(new SimpleFunction<KV<String, byte[]>, GenericRecord>() {
                    @Override
                    public GenericRecord apply(KV<String, byte[]> element) {
                        try {
                            DatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
                            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(element.getValue(), null);
                            return reader.read(null, decoder);
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to deserialize Avro message", e);
                        }
                    }
                }))
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
