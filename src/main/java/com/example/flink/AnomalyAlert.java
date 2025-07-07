package com.example.flink;

public class AnomalyAlert {
    private String symbol;
    private String message;
    private long timestamp;
    private double currentPrice;
    private double expectedPrice;

    // Default constructor
    public AnomalyAlert() {}

    // Constructor
    public AnomalyAlert(String symbol, String message, long timestamp, double currentPrice, double expectedPrice) {
        this.symbol = symbol;
        this.message = message;
        this.timestamp = timestamp;
        this.currentPrice = currentPrice;
        this.expectedPrice = expectedPrice;
    }

    // Getters and setters
    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getCurrentPrice() {
        return currentPrice;
    }

    public void setCurrentPrice(double currentPrice) {
        this.currentPrice = currentPrice;
    }

    public double getExpectedPrice() {
        return expectedPrice;
    }

    public void setExpectedPrice(double expectedPrice) {
        this.expectedPrice = expectedPrice;
    }

    @Override
    public String toString() {
        return "AnomalyAlert{" +
                "symbol='" + symbol + '\'' +
                ", message='" + message + '\'' +
                ", timestamp=" + timestamp +
                ", currentPrice=" + currentPrice +
                ", expectedPrice=" + expectedPrice +
                '}';
    }
}