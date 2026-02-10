package com.nashkod.eventscollector.service;

import com.nashkod.eventscollector.config.AppProperties;
import com.nashkod.eventscollector.metrics.CollectorMetrics;
import com.nashkod.eventscollector.model.OutboxRecord;
import com.nashkod.eventscollector.repository.ClickHouseRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class OutboxSchedulerTest {

    private RedisLockService lockService;
    private ClickHouseRepository repository;
    private RedisDedupService dedupService;
    private KafkaTemplate<String, String> kafkaTemplate;
    private CollectorMetrics metrics;
    private OutboxScheduler scheduler;

    @BeforeEach
    void setup() {
        lockService = mock(RedisLockService.class);
        repository = mock(ClickHouseRepository.class);
        dedupService = mock(RedisDedupService.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        metrics = mock(CollectorMetrics.class);

        AppProperties props = new AppProperties(
                new AppProperties.Topics("events", "devices"),
                new AppProperties.Outbox(100, 10000, 1000),
                new AppProperties.Redis("devices:seen", "devices:published", "outbox:lock")
        );

        scheduler = new OutboxScheduler(lockService, repository, dedupService, kafkaTemplate, props, metrics);
    }

    @Test
    void shouldPublishAndMarkSent() {
        OutboxRecord rec = new OutboxRecord("d-1", LocalDateTime.now(), 0);
        when(lockService.tryLock(anyString(), anyString(), any())).thenReturn(true);
        when(repository.findNewOutboxRecords(100)).thenReturn(List.of(rec));
        when(dedupService.markPublishedIfNew("d-1")).thenReturn(true);
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        scheduler.publishOutbox();

        verify(repository).markSent(rec);
        verify(metrics).incOutboxPublishSuccess();
    }

    @Test
    void shouldIncrementAttemptsOnFailure() {
        OutboxRecord rec = new OutboxRecord("d-1", LocalDateTime.now(), 0);
        when(lockService.tryLock(anyString(), anyString(), any())).thenReturn(true);
        when(repository.findNewOutboxRecords(100)).thenReturn(List.of(rec));
        when(dedupService.markPublishedIfNew("d-1")).thenReturn(true);
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenThrow(new RuntimeException("kafka down"));

        scheduler.publishOutbox();

        verify(repository).markFailed(eq(rec), contains("kafka down"));
        verify(metrics).incOutboxPublishFail();
    }
}
