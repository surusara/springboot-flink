package com.example.FlinkNettingApplication;

import java.io.Serializable;
import java.util.Objects;

public class CompositeKey implements Serializable {
    public String client;

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getBuySellDirection() {
        return buySellDirection;
    }

    public void setBuySellDirection(String buySellDirection) {
        this.buySellDirection = buySellDirection;
    }

    public String getSettlementDate() {
        return settlementDate;
    }

    public void setSettlementDate(String settlementDate) {
        this.settlementDate = settlementDate;
    }

    public String currency;
    public String buySellDirection;
    public String settlementDate;

    // Default constructor
    public CompositeKey() {}
    public CompositeKey(String client, String currency, String buySellDirection, String settlementDate) {
        this.client = client;
        this.currency = currency;
        this.buySellDirection = buySellDirection;
        this.settlementDate = settlementDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeKey that = (CompositeKey) o;
        return Objects.equals(client, that.client) &&
                Objects.equals(currency, that.currency) &&
                Objects.equals(buySellDirection, that.buySellDirection) &&
                Objects.equals(settlementDate, that.settlementDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(client, currency, buySellDirection, settlementDate);
    }
}
