package day12;

import org.apache.beam.sdk.coders.DefaultCoder;

//@DefaultCoder(SerializableCoder.class)
public class Transaction implements java.io.Serializable {
    public String transactionId;
    public String accountId;
    public double amount;
    public long timestamp;

    public Transaction(String transactionId, String accountId, double amount, long timestamp) {
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return transactionId + " | " + accountId + " | " + amount + " | " + timestamp;
    }
}
