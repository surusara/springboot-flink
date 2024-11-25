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

import java.io.*;
import java.util.Properties;

public class FlinkSqlNettingJob implements Serializable {
    private static final long serialVersionUID = 1L;

    // ObjectMapper for serialization/deserialization
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    public void execute() throws Exception {
        // Load properties from application.properties
        Properties appProperties = loadProperties("application.properties");

        // Set up Flink configuration and environment
        Configuration configuration = new Configuration();
        configuration.setString("class-name", "org.apache.flink.table.planner.delegation.BlinkExecutorFactory");
        configuration.setBoolean("streaming-mode", true);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka properties for Confluent Cloud
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", appProperties.getProperty("kafka.bootstrap.servers"));
        kafkaProperties.setProperty("group.id", appProperties.getProperty("kafka.group.id"));
        kafkaProperties.setProperty("security.protocol", appProperties.getProperty("kafka.security.protocol"));
        kafkaProperties.setProperty("sasl.mechanism", appProperties.getProperty("kafka.sasl.mechanism"));
        kafkaProperties.setProperty(
                "sasl.jaas.config",
                String.format(
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                        appProperties.getProperty("kafka.api.key"),
                        appProperties.getProperty("kafka.api.secret")
                )
        );
        kafkaProperties.setProperty("ssl.endpoint.identification.algorithm", appProperties.getProperty("kafka.ssl.endpoint.identification.algorithm"));

        // Add Schema Registry configuration
        kafkaProperties.setProperty("schema.registry.url", appProperties.getProperty("schema.registry.url"));
        kafkaProperties.setProperty("basic.auth.credentials.source", appProperties.getProperty("schema.registry.auth.credentials.source"));
        kafkaProperties.setProperty("basic.auth.user.info", appProperties.getProperty("schema.registry.auth.user.info"));

        // Kafka consumer for Avro deserialization
        FlinkKafkaConsumer<Trade> kafkaConsumer = new FlinkKafkaConsumer<>(
                appProperties.getProperty("kafka.input.topic"),
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

        KafkaSink<NettingResult> kafkaSink = KafkaSink.<NettingResult>builder()
                .setKafkaProducerConfig(kafkaProperties)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(appProperties.getProperty("kafka.output.topic"))
                                .setValueSerializationSchema(AvroSerializationSchema.forSpecific(NettingResult.class))
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Sink the results
        nettingResultStream.sinkTo(kafkaSink);

        // Execute the Flink job
        env.execute("Flink Netting Job with Avro and Confluent Cloud");
    }

    private Properties loadProperties(String fileName) throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName)) {
            if (inputStream == null) {
                throw new FileNotFoundException("Property file '" + fileName + "' not found in the classpath");
            }
            properties.load(inputStream);
        }
        return properties;
    }

}
