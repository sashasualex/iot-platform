package com.example.events_collector_service.kafka;

import com.example.events_collector_service.repository.ClickhouseEvents;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class EventsListener {

    private final ClickhouseEvents clickhouseEvents;

    @KafkaListener(topics = "${spring.kafka.consumer.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(GenericRecord record, Acknowledgment ack) {
        try {
            String eventId = record.get("eventId").toString();
            String deviceId = record.get("deviceId").toString();
            Long timestamp = Long.parseLong(record.get("timestamp").toString());
            String type = record.get("type").toString();
            String payload = record.get("payload").toString();

            clickhouseEvents.saveEvent(record);

            log.info("Received event: eventId={}, deviceId={}, timestamp={}, type={}, payload={}",
                    eventId, deviceId, timestamp, type, payload);

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Error processing eventId: {}", record.get("eventId"), e);
        }
    }
}
