package com.nashkod.eventscollector.metrics;

import com.nashkod.eventscollector.repository.ClickHouseRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class CollectorMetrics {

    private final Counter eventsProcessed;
    private final Counter eventsDuplicates;
    private final Counter outboxPublishSuccess;
    private final Counter outboxPublishFail;

    public CollectorMetrics(MeterRegistry meterRegistry, ClickHouseRepository clickHouseRepository) {
        this.eventsProcessed = Counter.builder("events.processed.total").register(meterRegistry);
        this.eventsDuplicates = Counter.builder("events.duplicates.total").register(meterRegistry);
        this.outboxPublishSuccess = Counter.builder("outbox.publish.success.total").register(meterRegistry);
        this.outboxPublishFail = Counter.builder("outbox.publish.fail.total").register(meterRegistry);
        Gauge.builder("outbox.pending.count", clickHouseRepository, ClickHouseRepository::pendingOutboxCount)
                .register(meterRegistry);
    }

    public void incEventsProcessed() { eventsProcessed.increment(); }
    public void incEventsDuplicates() { eventsDuplicates.increment(); }
    public void incOutboxPublishSuccess() { outboxPublishSuccess.increment(); }
    public void incOutboxPublishFail() { outboxPublishFail.increment(); }
}
