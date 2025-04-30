
package com.example.day2.json.types;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.processing.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "transferId",
    "status",
    "fromAccount",
    "toAccount",
    "amount"
})
@Generated("jsonschema2pojo")
public class JsonSchema {

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("transferId")
    private String transferId;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("status")
    private JsonSchema.Status status;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("fromAccount")
    private String fromAccount;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("toAccount")
    private String toAccount;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("amount")
    private Double amount;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("transferId")
    public String getTransferId() {
        return transferId;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("transferId")
    public void setTransferId(String transferId) {
        this.transferId = transferId;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("status")
    public JsonSchema.Status getStatus() {
        return status;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("status")
    public void setStatus(JsonSchema.Status status) {
        this.status = status;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("fromAccount")
    public String getFromAccount() {
        return fromAccount;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("fromAccount")
    public void setFromAccount(String fromAccount) {
        this.fromAccount = fromAccount;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("toAccount")
    public String getToAccount() {
        return toAccount;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("toAccount")
    public void setToAccount(String toAccount) {
        this.toAccount = toAccount;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("amount")
    public Double getAmount() {
        return amount;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("amount")
    public void setAmount(Double amount) {
        this.amount = amount;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(JsonSchema.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("transferId");
        sb.append('=');
        sb.append(((this.transferId == null)?"<null>":this.transferId));
        sb.append(',');
        sb.append("status");
        sb.append('=');
        sb.append(((this.status == null)?"<null>":this.status));
        sb.append(',');
        sb.append("fromAccount");
        sb.append('=');
        sb.append(((this.fromAccount == null)?"<null>":this.fromAccount));
        sb.append(',');
        sb.append("toAccount");
        sb.append('=');
        sb.append(((this.toAccount == null)?"<null>":this.toAccount));
        sb.append(',');
        sb.append("amount");
        sb.append('=');
        sb.append(((this.amount == null)?"<null>":this.amount));
        sb.append(',');
        sb.append("additionalProperties");
        sb.append('=');
        sb.append(((this.additionalProperties == null)?"<null>":this.additionalProperties));
        sb.append(',');
        if (sb.charAt((sb.length()- 1)) == ',') {
            sb.setCharAt((sb.length()- 1), ']');
        } else {
            sb.append(']');
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = ((result* 31)+((this.fromAccount == null)? 0 :this.fromAccount.hashCode()));
        result = ((result* 31)+((this.toAccount == null)? 0 :this.toAccount.hashCode()));
        result = ((result* 31)+((this.amount == null)? 0 :this.amount.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        result = ((result* 31)+((this.transferId == null)? 0 :this.transferId.hashCode()));
        result = ((result* 31)+((this.status == null)? 0 :this.status.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof JsonSchema) == false) {
            return false;
        }
        JsonSchema rhs = ((JsonSchema) other);
        return (((((((this.fromAccount == rhs.fromAccount)||((this.fromAccount!= null)&&this.fromAccount.equals(rhs.fromAccount)))&&((this.toAccount == rhs.toAccount)||((this.toAccount!= null)&&this.toAccount.equals(rhs.toAccount))))&&((this.amount == rhs.amount)||((this.amount!= null)&&this.amount.equals(rhs.amount))))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))))&&((this.transferId == rhs.transferId)||((this.transferId!= null)&&this.transferId.equals(rhs.transferId))))&&((this.status == rhs.status)||((this.status!= null)&&this.status.equals(rhs.status))));
    }

    @Generated("jsonschema2pojo")
    public enum Status {

        SUCCESS("SUCCESS"),
        FAILED("FAILED");
        private final String value;
        private final static Map<String, JsonSchema.Status> CONSTANTS = new HashMap<String, JsonSchema.Status>();

        static {
            for (JsonSchema.Status c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Status(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }

        @JsonValue
        public String value() {
            return this.value;
        }

        @JsonCreator
        public static JsonSchema.Status fromValue(String value) {
            JsonSchema.Status constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

}
