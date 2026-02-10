package com.nashkod.eventscollector.service;

import com.nashkod.avro.DeviceEvent;
import com.nashkod.eventscollector.metrics.CollectorMetrics;
import com.nashkod.eventscollector.repository.ClickHouseRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class EventProcessorTest {

    private ClickHouseRepository repository;
    private RedisDedupService dedupService;
    private CollectorMetrics metrics;
    private EventProcessor processor;

    @BeforeEach
    void setup() {
        repository = Mockito.mock(ClickHouseRepository.class);
        dedupService = Mockito.mock(RedisDedupService.class);
        metrics = Mockito.mock(CollectorMetrics.class);
        processor = new EventProcessor(repository, dedupService, metrics);
    }

    @Test
    void shouldCreateOutboxForNewDevice() {
        DeviceEvent event = DeviceEvent.newBuilder()
                .setEventId("e-1")
                .setDeviceId("d-1")
                .setTimestamp(1000L)
                .setType("temp")
                .setPayload("{}")
                .build();

        when(dedupService.markSeenIfNew("d-1")).thenReturn(true);

        processor.process(event);

        verify(repository).insertEvent(event);
        verify(repository).insertOutboxNew("d-1");
        verify(metrics, never()).incEventsDuplicates();
    }

    @Test
    void shouldNotCreateOutboxForExistingDevice() {
        DeviceEvent event = DeviceEvent.newBuilder()
                .setEventId("e-2")
                .setDeviceId("d-1")
                .setTimestamp(1000L)
                .setType("temp")
                .setPayload("{}")
                .build();

        when(dedupService.markSeenIfNew("d-1")).thenReturn(false);

        processor.process(event);

        verify(repository).insertEvent(any());
        verify(repository, never()).insertOutboxNew(any());
        verify(metrics).incEventsDuplicates();
    }
}
