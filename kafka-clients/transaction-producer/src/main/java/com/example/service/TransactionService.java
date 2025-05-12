package com.example.service;

import com.example.domain.Transaction;
import com.example.domain.TransactionKey;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
public class TransactionService {

//    private final KafkaProducer<TransactionKey, Transaction> kafkaProducer;

    @Value("${transaction-producer.topic-name}")
    private String topicName;

    private final KafkaTemplate<TransactionKey, Transaction> kafkaTemplate;
    List<String> regions = List.of("chennai", "bangalore", "mumbai", "delhi", "kolkata");

    public CompletableFuture<SendResult<TransactionKey, Transaction>> sendTransactionEvent(Transaction transaction) {
        ProducerRecord<TransactionKey, Transaction> producerRecord = new ProducerRecord<>(topicName, buildKey(transaction), transaction);
        // get random region
        String region = regions.get((int) (Math.random() * regions.size()));
        // add region as header
        producerRecord.headers().add("region", region.getBytes());
        CompletableFuture<SendResult<TransactionKey, Transaction>> completableFuture = kafkaTemplate.send(producerRecord);
        return completableFuture.whenComplete((result, ex) -> {
//            if (ex == null) {
//                System.out.println("Message sent successfully");
//            } else {
//                System.out.println("Message sent failed");
//            }
        });
    }

    private TransactionKey buildKey(Transaction transaction) {
        return TransactionKey.builder()
                .fromAccount(transaction.getFromAccount())
                .toAccount(transaction.getToAccount())
                .build();
    }
}
