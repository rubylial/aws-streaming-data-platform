package com.streaming.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.time.Instant;

/**
 * POJO del contrato de evento de transacción.
 *
 * Campos: event_id, event_time, merchant_id, amount, currency, status.
 * event_time se usa como Event Time para watermarks y ventanas.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TransactionEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("event_time")
    private String eventTime;

    @JsonProperty("merchant_id")
    private String merchantId;

    @JsonProperty("amount")
    private double amount;

    @JsonProperty("currency")
    private String currency;

    @JsonProperty("status")
    private String status;

    // --- Constructores ---

    public TransactionEvent() {}

    public TransactionEvent(String eventId, String eventTime, String merchantId,
                            double amount, String currency, String status) {
        this.eventId = eventId;
        this.eventTime = eventTime;
        this.merchantId = merchantId;
        this.amount = amount;
        this.currency = currency;
        this.status = status;
    }

    // --- Getters / Setters ---

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getEventTime() { return eventTime; }
    public void setEventTime(String eventTime) { this.eventTime = eventTime; }

    public String getMerchantId() { return merchantId; }
    public void setMerchantId(String merchantId) { this.merchantId = merchantId; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    /**
     * Convierte event_time ISO 8601 a epoch millis para Event Time.
     */
    public long getEventTimeMillis() {
        return Instant.parse(eventTime).toEpochMilli();
    }

    @Override
    public String toString() {
        return String.format("TransactionEvent{id=%s, time=%s, merchant=%s, amount=%.2f, status=%s}",
                eventId, eventTime, merchantId, amount, status);
    }
}
