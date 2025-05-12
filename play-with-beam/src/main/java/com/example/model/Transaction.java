package com.example.model;

import com.example.serde.CustomDateDeserilizer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

@Data
public class Transaction implements Serializable {
    private UUID id;
    private String fromAccount;
    private String toAccount;
    private double amount;
    @JsonDeserialize(using = CustomDateDeserilizer.class)
    private Date timestamp;
    private TransactionType type;
}
