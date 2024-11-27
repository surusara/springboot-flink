package com.example.FlinkNettingApplication;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NettingResultConsumer {

    @Autowired
    private NettingResultRepository repository;

    @KafkaListener(topics = "netting-results-topic", groupId = "netting-result-consumer-group")
    public void consume(NettingResult nettingResult) {
        System.out.println("Consumed message: " + nettingResult);

        // Upsert logic
        repository.findById(nettingResult.getPaymentId())
                .ifPresentOrElse(
                        existing -> {
                            existing.setClient(nettingResult.getClient());
                            existing.setCurrency(nettingResult.getCurrency());
                            existing.setBuySellDirection(nettingResult.getBuySellDirection());
                            existing.setSettlementDate(nettingResult.getSettlementDate());
                            existing.setNetConsideration(nettingResult.getNetConsideration());
                            existing.setState(nettingResult.getState());
                            repository.save(existing);
                        },
                        () -> repository.save(nettingResult)
                );
    }
}
