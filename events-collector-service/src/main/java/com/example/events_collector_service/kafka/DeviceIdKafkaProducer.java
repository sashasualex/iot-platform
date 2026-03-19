package com.example.events_collector_service.kafka;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class DeviceIdKafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${spring.kafka.producer.topic.name}")
    private String kafkaTopic;

    public boolean publishDeviceId(String deviceId) {
        CompletableFuture<SendResult<String, String>> send = kafkaTemplate.send(kafkaTopic, deviceId);
        try{
            send.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return true;
    }
}
