package com.example.FlinkNettingApplication;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NettingResultConsumer {

    @Autowired
    private NettingResultRepository repository;

    @KafkaListener(topics = "your-kafka-topic", groupId = "your-consumer-group")
    public void consumeNettingResult(NettingResultAvro nettingResultAvro) {
        // Map Avro object to Entity
        NettingResult nettingResult = mapToEntity(nettingResultAvro);

        // Save or update in the database
        nettingResultRepository.save(nettingResult);

        System.out.println("Persisted NettingResult: " + nettingResult);
    }

    private NettingResult mapToEntity(NettingResultAvro avroObject) {
        return new NettingResult(
                avroObject.getClient(),
                avroObject.getCurrency(),
                avroObject.getBuySellDirection(),
                avroObject.getSettlementDate(),
                avroObject.getNetConsideration(),
                avroObject.getPaymentId(),
                avroObject.getState()
        );
    }

}
