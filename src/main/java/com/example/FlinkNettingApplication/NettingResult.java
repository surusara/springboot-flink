package com.example.FlinkNettingApplication;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Properties;
// POJO for NettingResult
public  class NettingResult {
    public String client;
    public String currency;
    public String buySellDirection;
    public String settlementDate;
    public double netConsideration;
    public String paymentId;
    public String state;  // "Initial", "Intermediate", or "Final"
    public NettingResult() {
    }

    public NettingResult(String client, String currency, String buySellDirection, String settlementDate, double netConsideration,String paymentId, String state) {
        this.client = client;
        this.currency = currency;
        this.buySellDirection = buySellDirection;
        this.settlementDate = settlementDate;
        this.netConsideration = netConsideration;
        this.paymentId=paymentId;
        this.state =state;
    }
}