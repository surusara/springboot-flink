package com.example.FlinkNettingApplication;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.Serializable;
import java.util.Properties;

public class FlinkSqlNettingJob implements Serializable {
    final long serialVersionUID = 1L;

    // Example ObjectMapper configuration
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule()); // Register the module
    public void execute() throws Exception {
        // Set up execution environment

        org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();

        // Set class name for Blink Executor Factory
        configuration.setString("class-name", "org.apache.flink.table.planner.delegation.BlinkExecutorFactory");

        // Set streaming mode to true
        configuration.setBoolean("streaming-mode", true);

        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();;

        // Kafka consumer configuration
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:29092");
        kafkaProperties.setProperty("group.id", "flink_group1");

        // Create a Kafka consumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "trade_topic11",
                new SimpleStringSchema(),
                kafkaProperties
        );

        // Assign timestamps and watermarks based on eventTime
        kafkaConsumer.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(5))
                        .withTimestampAssigner((message, timestamp) -> {
                            try {
                                ObjectMapper objectMapper = new ObjectMapper();
                                Trade trade = objectMapper.readValue(message, Trade.class);
                                return trade.eventTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
                            } catch (Exception e) {
                                return Long.MIN_VALUE;
                            }
                        })
        );

        // Consume data from Kafka
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        // Process the Kafka messages
        DataStream<Trade> tradeStream = kafkaStream
                .process(new ProcessFunction<String, Trade>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<Trade> out) throws Exception {
                        Trade trade = objectMapper.readValue(value, Trade.class); // Use the configured ObjectMapper
                        out.collect(trade);
                    }
                });

        // Aggregate netting results
            DataStream<NettingResult> nettingResultStream = tradeStream
                .keyBy(trade -> new CompositeKey(
                        trade.client,
                        trade.currency,
                        trade.buySellDirection,
                        trade.settlementDate
                ))
                .process(new NettingProcessFunction());

        // Produce the results back to Kafka
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "netting_results11",
                new SimpleStringSchema(),
                kafkaProperties
        );

        // Convert NettingResult to JSON string before sending to Kafka
        nettingResultStream
                .map(result -> new ObjectMapper().writeValueAsString(result))
                .addSink(kafkaProducer);

        // Execute the Flink job
        env.execute("Flink Netting Job");
    }
}