import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;

Properties kafkaProperties = new Properties();
kafkaProperties.setProperty("bootstrap.servers", "<BROKER_URL>");
kafkaProperties.setProperty("group.id", "<CONSUMER_GROUP>");
kafkaProperties.setProperty("schema.registry.url", "<CONFLUENT_SCHEMA_REGISTRY_URL>");
kafkaProperties.setProperty("basic.auth.credentials.source", "USER_INFO");
kafkaProperties.setProperty("basic.auth.user.info", "<SCHEMA_REGISTRY_API_KEY>:<SCHEMA_REGISTRY_API_SECRET>");
kafkaProperties.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true"); // For SpecificRecord support

FlinkKafkaConsumer<SettlementObligation> kafkaConsumer = new FlinkKafkaConsumer<>(
        "<KAFKA_TOPIC>",
        ConfluentRegistryAvroDeserializationSchema.forSpecific(SettlementObligation.class, "<CONFLUENT_SCHEMA_REGISTRY_URL>"),
        kafkaProperties
);

kafkaConsumer.setStartFromLatest(); // Optional: Start from latest messages


In your Flink job, log the deserialized object type to confirm it's being mapped to SettlementObligation:
tradeStream.map(trade -> {
        System.out.println("Deserialized Object Type: " + trade.getClass().getName());
        return trade;
});
