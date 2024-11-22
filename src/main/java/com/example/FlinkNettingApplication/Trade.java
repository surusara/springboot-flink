package com.example.FlinkNettingApplication;
import java.time.LocalDateTime;

// POJO for Trade
public  class Trade {
    public String client;
    public String currency;
    public String buySellDirection;
    public String settlementDate; // String is fine if you handle parsing elsewhere
    public double consideration;
    public String operation;
    public LocalDateTime eventTime; // LocalDateTime for the timestamp

    // Default constructor for deserialization
    public Trade() {
    }
}