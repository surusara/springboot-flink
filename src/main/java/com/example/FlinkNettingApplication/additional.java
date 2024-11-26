import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

public class FlinkKafkaConsumerConfluentCloud {

    public static void main(String[] args) throws Exception {
        // Step 1: Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka and Schema Registry properties
        String bootstrapServers = "<YOUR_CONFLUENT_CLOUD_BOOTSTRAP_SERVERS>";
        String schemaRegistryUrl = "<YOUR_SCHEMA_REGISTRY_URL>";
        String consumerGroupId = "<YOUR_CONSUMER_GROUP>";

        // Schema Registry authentication properties
        Map<String, String> schemaRegistryConfig = new HashMap<>();
        schemaRegistryConfig.put("basic.auth.credentials.source", "USER_INFO");
        schemaRegistryConfig.put("basic.auth.user.info", "<YOUR_SCHEMA_REGISTRY_API_KEY>:<YOUR_SCHEMA_REGISTRY_API_SECRET>");
        schemaRegistryConfig.put("schema.registry.url", schemaRegistryUrl);

        // Step 2: Kafka Source with Confluent Schema Registry for Avro Deserialization
        KafkaSource<SettlementObligation> kafkaSource = KafkaSource.<SettlementObligation>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("<YOUR_TOPIC>")
                .setGroupId(consumerGroupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets()) // Start from last committed offset
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                SettlementObligation.class,
                                schemaRegistryUrl,
                                schemaRegistryConfig // Pass the configuration map
                        )
                )
                .build();

        // Step 3: Add Kafka Source to Flink pipeline
        env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(), // Configure as per timestamping needs
                "Confluent Cloud Kafka Source"
        ).map(settlementObligation -> {
            // Process the Settlement Obligation object
            System.out.println("Received Settlement Obligation: " + settlementObligation);
            return settlementObligation;
        });

        // Step 4: Execute the Flink job
        env.execute("Flink Kafka Consumer with Confluent Cloud");
    }
}
