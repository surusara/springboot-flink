package com.example.FlinkNettingApplication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class FlinkNettingJobWithKafkaSource {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    public void execute() throws Exception {
        // Load properties from application.properties
        Properties appProperties = loadProperties("application.properties");

        // Set up Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka properties
        String bootstrapServers = appProperties.getProperty("kafka.bootstrap.servers");
        String schemaRegistryUrl = appProperties.getProperty("schema.registry.url");
        String inputTopic = appProperties.getProperty("kafka.input.topic");
        String outputTopic = appProperties.getProperty("kafka.output.topic");
        String consumerGroupId = appProperties.getProperty("kafka.group.id");

        // Schema Registry configuration
        Properties schemaRegistryConfig = new Properties();
        schemaRegistryConfig.put("basic.auth.credentials.source", appProperties.getProperty("schema.registry.auth.credentials.source"));
        schemaRegistryConfig.put("basic.auth.user.info", appProperties.getProperty("schema.registry.auth.user.info"));

        // Kafka Source with Avro deserialization
        KafkaSource<SettlementObligation> kafkaSource = KafkaSource.<SettlementObligation>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(consumerGroupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets()) // Consume from last committed offsets
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                SettlementObligation.class,
                                schemaRegistryUrl,
                                schemaRegistryConfig
                        )
                )
                .build();

        // Trade stream from Kafka
        DataStream<SettlementObligation> tradeStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<SettlementObligation>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(5))
                        .withTimestampAssigner((trade, timestamp) ->
                                trade.getEvent().getTimestamp().toEpochMilli() // Use event timestamp
                        ),
                "Kafka Source"
        ).map(trade -> {
            System.out.println("Received trade: " + trade);
            return trade;
        });

        // Netting Process
        DataStream<NettingResult> nettingResultStream = tradeStream
                .keyBy(trade -> new CompositeKey(
                        trade.getClient(),
                        trade.getCurrency(),
                        trade.getBuySellDirection(),
                        trade.getSettlementDate()
                ))
                .process(new NettingProcessFunction());

        // Kafka Sink with Avro serialization
        KafkaSink<NettingResult> kafkaSink = KafkaSink.<NettingResult>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(AvroSerializationSchema.forSpecific(NettingResult.class))
                                .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Sink the results
        nettingResultStream.sinkTo(kafkaSink);

        // Execute the Flink job
        env.execute("Flink Netting Job with Kafka Source and Confluent Schema Registry");
    }

    private Properties loadProperties(String fileName) throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName)) {
            if (inputStream == null) {
                throw new IOException("Property file '" + fileName + "' not found in the classpath");
            }
            properties.load(inputStream);
        }
        return properties;
    }
}
