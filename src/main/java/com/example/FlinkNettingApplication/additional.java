import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class FlinkKafkaConsumerConfluentCloud {

    public static void main(String[] args) throws Exception {
        // Step 1: Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Step 2: Kafka properties for Confluent Cloud
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "<YOUR_CONFLUENT_CLOUD_BOOTSTRAP_SERVERS>");
        kafkaProperties.setProperty("group.id", "<YOUR_CONSUMER_GROUP>");
        kafkaProperties.setProperty("security.protocol", "SASL_SSL");
        kafkaProperties.setProperty("sasl.mechanism", "PLAIN");
        kafkaProperties.setProperty(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"<YOUR_API_KEY>\" " +
                        "password=\"<YOUR_API_SECRET>\";"
        );
        kafkaProperties.setProperty("specific.avro.reader", "true");
        kafkaProperties.setProperty("ssl.endpoint.identification.algorithm", "https");
        kafkaProperties.setProperty("schema.registry.url", "<YOUR_SCHEMA_REGISTRY_URL>");
        kafkaProperties.setProperty("basic.auth.credentials.source", "USER_INFO");
        kafkaProperties.setProperty("basic.auth.user.info", "<YOUR_SCHEMA_REGISTRY_API_KEY>:<YOUR_SCHEMA_REGISTRY_API_SECRET>");

        // Step 3: Kafka Source with Confluent Schema Registry for Avro Deserialization
        KafkaSource<SettlementObligation> kafkaSource = KafkaSource.<SettlementObligation>builder()
                .setBootstrapServers(kafkaProperties.getProperty("bootstrap.servers"))
                .setTopics("<YOUR_TOPIC>")
                .setGroupId(kafkaProperties.getProperty("group.id"))
                .setStartingOffsets(OffsetsInitializer.committedOffsets()) // Start from last committed offset
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                SettlementObligation.class,
                                kafkaProperties.getProperty("schema.registry.url"),
                                kafkaProperties
                        )
                )
                .build();

        // Step 4: Add Kafka Source to Flink pipeline
        env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(), // Configure as per timestamping needs
                "Confluent Cloud Kafka Source"
        ).map(settlementObligation -> {
            // Process the Settlement Obligation object
            System.out.println("Received Settlement Obligation: " + settlementObligation);
            return settlementObligation;
        });

        // Step 5: Execute the Flink job
        env.execute("Flink Kafka Consumer with Confluent Cloud");
    }
}


<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>1.16.2</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-avro-confluent-registry</artifactId>
    <version>1.16.2</version>
</dependency>
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>7.4.0</version> <!-- Ensure version matches your Kafka setup -->
</dependency>