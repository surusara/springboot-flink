package com.example.FlinkNettingApplication;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NettingResult> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, NettingResult> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    public DefaultKafkaConsumerFactory<String, NettingResult> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "<CONFLUENT_CLOUD_BOOTSTRAP_SERVERS>");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "netting-result-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put("schema.registry.url", "<SCHEMA_REGISTRY_URL>");
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", "<API_KEY>:<API_SECRET>");
        props.put("specific.avro.reader", true); // Maps to generated Avro classes

        return new DefaultKafkaConsumerFactory<>(props);
    }
}
