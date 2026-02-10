package com.nashkod.eventscollector.service;

import com.nashkod.avro.DeviceEvent;
import com.nashkod.eventscollector.metrics.CollectorMetrics;
import com.nashkod.eventscollector.repository.ClickHouseRepository;
import org.springframework.stereotype.Service;

@Service
public class EventProcessor {

    private final ClickHouseRepository clickHouseRepository;
    private final RedisDedupService dedupService;
    private final CollectorMetrics metrics;

    public EventProcessor(ClickHouseRepository clickHouseRepository, RedisDedupService dedupService, CollectorMetrics metrics) {
        this.clickHouseRepository = clickHouseRepository;
        this.dedupService = dedupService;
        this.metrics = metrics;
    }

    public void process(DeviceEvent event) {
        clickHouseRepository.insertEvent(event);
        metrics.incEventsProcessed();
        if (dedupService.markSeenIfNew(event.getDeviceId().toString())) {
            clickHouseRepository.insertOutboxNew(event.getDeviceId().toString());
        } else {
            metrics.incEventsDuplicates();
        }
    }
}
