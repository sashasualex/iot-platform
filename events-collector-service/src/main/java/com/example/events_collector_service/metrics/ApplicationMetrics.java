package com.example.events_collector_service.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ApplicationMetrics {

    private final MeterRegistry meterRegistry;

    private Counter howManyEventsWereAcceptedFromKafka;
    private Counter howManyEventsWereRecordedInTheOutbox;
    private Counter howManyDeviceIdsWereSentToDevicesTopic;
    private Counter howManyErrorsAreThereWhenSendingToDevicesTopic;


    @PostConstruct()
    public void init() {
        this.howManyEventsWereAcceptedFromKafka = meterRegistry.counter("events_ingested");
        this.howManyEventsWereRecordedInTheOutbox = meterRegistry.counter("outbox_saved");
        this.howManyDeviceIdsWereSentToDevicesTopic = meterRegistry.counter("devices_published");
        this.howManyErrorsAreThereWhenSendingToDevicesTopic = meterRegistry.counter("devices_publish_errors");
    }

    public void incEventsIngested() {
        this.howManyEventsWereAcceptedFromKafka.increment();
    }

    public void incOutboxCreated() {
        this.howManyEventsWereRecordedInTheOutbox.increment();
    }

    public void incDevicesPublished() {
        this.howManyDeviceIdsWereSentToDevicesTopic.increment();
    }

    public void incDevicesPublishedError() {
        this.howManyErrorsAreThereWhenSendingToDevicesTopic.increment();
    }
}

