package com.example.util;

import com.example.domain.Transaction;
import com.example.domain.TransactionType;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.core.io.ClassPathResource;

import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TransactionLoader {

    private static final CSVFormat CSV_FORMAT = CSVFormat.RFC4180.builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .build();

    @SneakyThrows
    public static List<Transaction> loadTransactions(String filename) {
        ClassPathResource resource = new ClassPathResource(filename);
        try (Reader reader = new InputStreamReader(resource.getInputStream());
                CSVParser parser = new CSVParser(reader, CSV_FORMAT)) {

            return parser.getRecords().stream()
                    .map(TransactionLoader::mapRecordToTransaction)
                    .toList();
        }
    }

    private static Transaction mapRecordToTransaction(CSVRecord csvRecord) {
        return Transaction.builder()
                .id(UUID.randomUUID())
                .timestamp(parseDateTime(csvRecord.get(0)))
                .type(TransactionType.valueOf(csvRecord.get(1)))
                .fromAccount(extractAccount(csvRecord.get(2)))
                .toAccount(extractAccount(csvRecord.get(3)))
                .amount(new BigDecimal(csvRecord.get(4)))
                .build();
    }

    private static String extractAccount(String account) {
        return (Objects.isNull(account) || account.isBlank()) ? null : account;
    }

    private static LocalDateTime parseDateTime(String timestamp) {
        return LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
