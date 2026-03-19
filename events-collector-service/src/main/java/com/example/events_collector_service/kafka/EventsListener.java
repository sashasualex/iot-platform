package com.example.events_collector_service.kafka;

import com.example.events_collector_service.metrics.ApplicationMetrics;
import com.example.events_collector_service.repository.DeviceEventsRepository;
import com.example.events_collector_service.repository.DeviceOutboxRepository;
import com.example.events_collector_service.service.DeviceDedupService;
import com.nashkod.avro.DeviceEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class EventsListener {

    private final DeviceEventsRepository deviceEventsRepository;
    private final DeviceDedupService deviceDedupService;
    private final DeviceOutboxRepository deviceOutboxRepository;
    private final ApplicationMetrics metrics;

    @KafkaListener(topics = "${spring.kafka.consumer.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(DeviceEvent event, Acknowledgment ack) {
        metrics.incEventsIngested();
        try {
            String eventId = event.get("eventId").toString();
            String deviceId = event.get("deviceId").toString();
            Long timestamp = Long.parseLong(event.get("timestamp").toString());
            String type = event.get("type").toString();
            String payload = event.get("payload").toString();

            deviceEventsRepository.saveEvent(event);

            boolean check = deviceDedupService.addDeviceIdAndReturnStatus(deviceId);

            if (check){
                deviceOutboxRepository.saveOutbox(deviceId);
                metrics.incOutboxCreated();
            }
            log.info("Received event: eventId={}, deviceId={}, timestamp={}, type={}, payload={}",
                    eventId, deviceId, timestamp, type, payload);

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Error processing eventId: {}", event.get("eventId"), e);
            throw new RuntimeException(e);
        }
    }
}
