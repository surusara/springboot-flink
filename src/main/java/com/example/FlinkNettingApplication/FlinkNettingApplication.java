package com.example.FlinkNettingApplication;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FlinkNettingApplication {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(FlinkNettingApplication.class, args);
		//StreamTableEnvironment tableEnv = FlinkTableEnvSingleton.getInstance();
		FlinkSqlNettingJob jobExecutor = new FlinkSqlNettingJob();
		jobExecutor.execute();
	}

}
