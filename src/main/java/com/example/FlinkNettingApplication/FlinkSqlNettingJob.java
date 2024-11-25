package com.example.FlinkNettingApplication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.AvroSerializationSchema;

import java.io.Serializable;
import java.util.Properties;

public class FlinkSqlNettingJob implements Serializable {
    private static final long serialVersionUID = 1L;

    // ObjectMapper for serialization/deserialization
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    public void execute() throws Exception {
        // Set up Flink configuration and environment
        Configuration configuration = new Configuration();
        configuration.setString("class-name", "org.apache.flink.table.planner.delegation.BlinkExecutorFactory");
        configuration.setBoolean("streaming-mode", true);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka properties for Confluent Cloud
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "<YOUR_CONFLUENT_CLOUD_BOOTSTRAP_SERVERS>");
        kafkaProperties.setProperty("group.id", "flink_group1");
        kafkaProperties.setProperty("security.protocol", "SASL_SSL");
        kafkaProperties.setProperty("sasl.mechanism", "PLAIN");
        kafkaProperties.setProperty(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"<YOUR_API_KEY>\" " +
                        "password=\"<YOUR_API_SECRET>\";"
        );
        kafkaProperties.setProperty("ssl.endpoint.identification.algorithm", "https");

        // Kafka consumer for Avro deserialization
        FlinkKafkaConsumer<Trade> kafkaConsumer = new FlinkKafkaConsumer<>(
                "trade_topic15",
                AvroDeserializationSchema.forSpecific(Trade.class),
                kafkaProperties
        );

        kafkaConsumer.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Trade>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(5))
                        .withTimestampAssigner((trade, timestamp) ->
                                trade.getEventTime() != null ? trade.getEventTime().toEpochMilli() : Long.MIN_VALUE
                        )
        );

        // Trade stream from Kafka
        DataStream<Trade> tradeStream = env.addSource(kafkaConsumer)
                .map(trade -> {
                    System.out.println("Received trade: " + trade);
                    return trade;
                });

        // Process trades for netting
        DataStream<NettingResult> nettingResultStream = tradeStream
                .keyBy(trade -> new CompositeKey(
                        (String) trade.getClient(),
                        (String) trade.getCurrency(),
                        (String) trade.getBuySellDirection(),
                        (String) trade.getSettlementDate()
                ))
                .process(new NettingProcessFunction());

        // Define Kafka sink for Confluent Cloud
        KafkaSink<NettingResult> kafkaSink = KafkaSink.<NettingResult>builder()
                .setBootstrapServers("<YOUR_CONFLUENT_CLOUD_BOOTSTRAP_SERVERS>")
                .setKafkaProducerConfig(kafkaProperties) // Use the same properties as the consumer
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("netting_results15")
                                .setValueSerializationSchema(AvroSerializationSchema.forSpecific(NettingResult.class))
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // Ensure at-least-once delivery
                .build();

        // Sink the results
        nettingResultStream.sinkTo(kafkaSink);

        // Execute the Flink job
        env.execute("Flink Netting Job with Avro and Confluent Cloud");
    }
}
