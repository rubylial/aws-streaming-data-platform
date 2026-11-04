package com.streaming.flink;

import java.io.Serializable;

/**
 * Resultado de agregación por merchant en una ventana.
 *
 * Contiene: merchantId, windowStart, windowEnd,
 * transactionCount, totalAmount, avgAmount.
 */
public class MerchantAggregation implements Serializable {

    private static final long serialVersionUID = 1L;

    private String merchantId;
    private String windowStart;
    private String windowEnd;
    private long transactionCount;
    private double totalAmount;
    private double avgAmount;

    public MerchantAggregation() {}

    public MerchantAggregation(String merchantId, String windowStart, String windowEnd,
                                long transactionCount, double totalAmount, double avgAmount) {
        this.merchantId = merchantId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.transactionCount = transactionCount;
        this.totalAmount = totalAmount;
        this.avgAmount = avgAmount;
    }

    // --- Getters / Setters ---

    public String getMerchantId() { return merchantId; }
    public void setMerchantId(String merchantId) { this.merchantId = merchantId; }

    public String getWindowStart() { return windowStart; }
    public void setWindowStart(String windowStart) { this.windowStart = windowStart; }

    public String getWindowEnd() { return windowEnd; }
    public void setWindowEnd(String windowEnd) { this.windowEnd = windowEnd; }

    public long getTransactionCount() { return transactionCount; }
    public void setTransactionCount(long transactionCount) { this.transactionCount = transactionCount; }

    public double getTotalAmount() { return totalAmount; }
    public void setTotalAmount(double totalAmount) { this.totalAmount = totalAmount; }

    public double getAvgAmount() { return avgAmount; }
    public void setAvgAmount(double avgAmount) { this.avgAmount = avgAmount; }

    @Override
    public String toString() {
        return String.format(
            "MerchantAgg{merchant=%s, window=[%s -> %s], count=%d, total=%.2f, avg=%.2f}",
            merchantId, windowStart, windowEnd, transactionCount, totalAmount, avgAmount
        );
    }
}
