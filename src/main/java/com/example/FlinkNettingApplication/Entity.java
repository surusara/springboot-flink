package com.example.FlinkNettingApplication;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class NettingResult {
    @Id
    private String paymentId;
    private String client;
    private String currency;
    private String buySellDirection;
    private String settlementDate;
    private double netConsideration;
    private String state;
}
