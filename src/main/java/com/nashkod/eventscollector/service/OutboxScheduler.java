package com.nashkod.eventscollector.service;

import com.nashkod.eventscollector.config.AppProperties;
import com.nashkod.eventscollector.metrics.CollectorMetrics;
import com.nashkod.eventscollector.model.OutboxRecord;
import com.nashkod.eventscollector.repository.ClickHouseRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

@Component
public class OutboxScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxScheduler.class);

    private final RedisLockService redisLockService;
    private final ClickHouseRepository clickHouseRepository;
    private final RedisDedupService redisDedupService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AppProperties properties;
    private final CollectorMetrics metrics;

    public OutboxScheduler(
            RedisLockService redisLockService,
            ClickHouseRepository clickHouseRepository,
            RedisDedupService redisDedupService,
            KafkaTemplate<String, String> kafkaTemplate,
            AppProperties properties,
            CollectorMetrics metrics
    ) {
        this.redisLockService = redisLockService;
        this.clickHouseRepository = clickHouseRepository;
        this.redisDedupService = redisDedupService;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${app.outbox.cron-delay-ms:5000}")
    public void publishOutbox() {
        String token = UUID.randomUUID().toString();
        String lockKey = properties.redis().outboxLockKey();
        boolean locked = redisLockService.tryLock(lockKey, token, Duration.ofMillis(properties.outbox().lockTtlMs()));
        if (!locked) {
            return;
        }
        try {
            List<OutboxRecord> records = clickHouseRepository.findNewOutboxRecords(properties.outbox().batchSize());
            for (OutboxRecord record : records) {
                try {
                    if (redisDedupService.markPublishedIfNew(record.deviceId())) {
                        kafkaTemplate.send(properties.topics().devices(), record.deviceId(), record.deviceId()).get();
                    }
                    clickHouseRepository.markSent(record);
                    metrics.incOutboxPublishSuccess();
                } catch (Exception e) {
                    clickHouseRepository.markFailed(record, e.getMessage());
                    metrics.incOutboxPublishFail();
                    log.warn("Failed to publish outbox for deviceId={}", record.deviceId(), e);
                }
            }
        } finally {
            redisLockService.unlock(lockKey, token);
        }
    }
}
