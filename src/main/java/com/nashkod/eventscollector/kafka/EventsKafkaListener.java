package com.nashkod.eventscollector.kafka;

import com.nashkod.avro.DeviceEvent;
import com.nashkod.eventscollector.service.EventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class EventsKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(EventsKafkaListener.class);

    private final EventProcessor eventProcessor;

    public EventsKafkaListener(EventProcessor eventProcessor) {
        this.eventProcessor = eventProcessor;
    }

    @KafkaListener(topics = "${app.topics.events}", containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(DeviceEvent event, Acknowledgment ack) {
        log.info("Got event eventId={} deviceId={}", event.getEventId(), event.getDeviceId());
        eventProcessor.process(event);
        ack.acknowledge();
    }
}
