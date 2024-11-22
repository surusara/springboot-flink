package com.example.FlinkNettingApplication;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

@Configuration
public class FlinkConfig {

    @Bean
    public static StreamExecutionEnvironment streamExecutionEnvironment() {
        org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();

        // Set class name for Blink Executor Factory
        configuration.setString("class-name", "org.apache.flink.table.planner.delegation.BlinkExecutorFactory");

        // Set streaming mode to true
        configuration.setBoolean("streaming-mode", true);

        // Set up the streaming execution environment
        return StreamExecutionEnvironment.createLocalEnvironment();
    }

    @Bean
    public static StreamTableEnvironment streamTableEnvironment(StreamExecutionEnvironment streamEnv) {
// Configure the TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode() // Specify that we are in streaming mode
                .withBuiltInCatalogName("default_catalog")
                .withBuiltInDatabaseName("default_database")
                .build();

        return  StreamTableEnvironment.create(streamEnv, settings);
    }
}

