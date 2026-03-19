package com.example.events_collector_service.scheduler;

import com.example.events_collector_service.metrics.ApplicationMetrics;
import com.example.events_collector_service.redis.RedisLockService;
import com.example.events_collector_service.redis.PublishedDeviceIdsRedisService;
import com.example.events_collector_service.repository.DeviceOutboxRepository;
import com.example.events_collector_service.kafka.DeviceIdKafkaProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;
import java.util.UUID;

@Configuration
@EnableScheduling
@RequiredArgsConstructor
@Slf4j
public class OutboxScheduler {
    private final JdbcTemplate jdbcTemplate;
    private final RedisLockService redisLockService;
    private final DeviceIdKafkaProducer kafkaPublishService;
    private final DeviceOutboxRepository deviceOutboxRepository;
    private final PublishedDeviceIdsRedisService redisPublishDeviceId;
    private final ApplicationMetrics metrics;

    @Value("${spring.scheduling.batchSize}")
    private long batchSize = 10;
    private String token = UUID.randomUUID().toString();

    @Scheduled(cron = "${spring.scheduling.outboxCron}")
    public void searchDeviceIdInTableDeviceOutbox() {
        String sql = """
                SELECT device_id FROM events.device_outbox 
                WHERE status=0 
                ORDER BY created_at 
                LIMIT ?
                """;

        if(redisLockService.tryLock(token)) {
            log.info("Lock acquired, processing the outbox with token: {}", token);
            try {
                List<String> listDeviceId = jdbcTemplate.queryForList(sql, String.class, batchSize);
                log.info("number of device_ids found in the device_outbox table: {}", listDeviceId.size());

                for (String deviceId : listDeviceId) {
                    try {
                        if (redisPublishDeviceId.publishDeviceId(deviceId) == 1L) {
                            kafkaPublishService.publishDeviceId(deviceId);
                            metrics.incDevicesPublished();
                        }
                        deviceOutboxRepository.markSent(deviceId);
                    } catch (Exception e) {
                        log.error("markSent failed for deviceId={}", deviceId, e);
                        metrics.incDevicesPublishedError();
                        deviceOutboxRepository.markFailed(deviceId, e.getMessage());
                        redisPublishDeviceId.deleteDeviceId(deviceId);
                    }
                }
            } finally {
                redisLockService.unlock(token);
                log.info("Unlock acquired, processing the outbox");
            }
        } else{
            log.info("Lock skipped, another instance is processing the outbox");
        }
    }

}
