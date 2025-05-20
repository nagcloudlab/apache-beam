package day13;


import java.io.Serializable;
import org.joda.time.Instant;
import java.util.Objects;

public class Transaction implements Serializable {

    private final String transactionId;
    private final String accountId;
    private final double amount;
    private final Instant timestamp;

    public Transaction(String transactionId, String accountId, double amount, Instant timestamp) {
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    public String getTransactionId() { return transactionId; }
    public String getAccountId() { return accountId; }
    public double getAmount() { return amount; }
    public Instant getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return String.format("Transaction(%s, %s, %.2f, %s)", transactionId, accountId, amount, timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Transaction)) return false;
        Transaction t = (Transaction) o;
        return Double.compare(t.amount, amount) == 0 &&
                transactionId.equals(t.transactionId) &&
                accountId.equals(t.accountId) &&
                timestamp.equals(t.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, accountId, amount, timestamp);
    }

    public static Transaction of(String txId, String accountId, double amount, Instant timestamp) {
        return new Transaction(txId, accountId, amount, timestamp);
    }

}