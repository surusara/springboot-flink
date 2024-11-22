package com.example.FlinkNettingApplication;

import java.io.Serializable;
import java.util.Objects;

public class CompositeKey implements Serializable {
    public String client;
    public String currency;
    public String buySellDirection;
    public String settlementDate;

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
