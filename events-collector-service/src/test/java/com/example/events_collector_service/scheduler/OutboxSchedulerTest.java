package com.example.events_collector_service.scheduler;

import com.example.events_collector_service.redis.PublishedDeviceIdsRedisService;
import com.example.events_collector_service.redis.RedisLockService;
import com.example.events_collector_service.repository.DeviceOutboxRepository;
import com.example.events_collector_service.kafka.DeviceIdKafkaProducer;
import com.example.events_collector_service.utils.DeviceDedupServiceTestUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.BDDMockito;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class OutboxSchedulerTest {

    @Mock
    private JdbcTemplate jdbcTemplate;
    @Mock
    private RedisLockService redisLockService;
    @Mock
    private DeviceIdKafkaProducer kafkaPublishService;
    @Mock
    private DeviceOutboxRepository deviceOutboxRepository;
    @Mock
    private PublishedDeviceIdsRedisService redisPublishService;

    @InjectMocks
    private OutboxScheduler outboxScheduler;

    @Test
    @DisplayName("Process the outbox when lock is acquired and there are device ids in the outbox, then publish to redis and kafka, mark sent and unlock")
    public void givenLockAcquiredAndOutboxHasDeviceIds_whenRunScheduler_thenPublishAndMarkSentAndUnlock() {
        //given
        String deviceId1 = DeviceDedupServiceTestUtils.getDeviceId1();
        String deviceId2 = DeviceDedupServiceTestUtils.getDeviceId2();
        String deviceId3 = DeviceDedupServiceTestUtils.getDeviceId3();
        List<String> deviceIds = List.of(deviceId1, deviceId2, deviceId3);
        BDDMockito.given(redisLockService.tryLock(anyString()))
                .willReturn(true);
        BDDMockito.given(jdbcTemplate.queryForList(anyString(), eq(String.class), anyLong()))
                .willReturn(deviceIds);
        BDDMockito.given(redisPublishService.publishDeviceId(argThat(id -> id != null && deviceIds.contains(id))))
                .willReturn(1L);
        //when
        outboxScheduler.searchDeviceIdInTableDeviceOutbox();
        //then
        verify(redisLockService, times(1)).tryLock(anyString());
        verify(jdbcTemplate, times(1)).queryForList(anyString(), eq(String.class), anyLong());
        verify(redisPublishService, times(deviceIds.size())).publishDeviceId(anyString());
        verify(kafkaPublishService, times(deviceIds.size())).publishDeviceId(anyString());
        verify(deviceOutboxRepository, times(deviceIds.size())).markSent(anyString());
        verify(deviceOutboxRepository, times(0)).markFailed(anyString(), anyString());
        verify(redisPublishService, times(0)).deleteDeviceId(anyString());
        verify(redisLockService, times(1)).unlock(anyString());

    }

    @Test
    @DisplayName("Give device id exists in redis, skip to publish device id to kafka and mark sent but not delete device id in redis")
    public void givenDeviceIdExistsInRedis_whenRunScheduler_thenSkipToPublishDeviceIdToKafka() {
        //given
        String deviceId1 = DeviceDedupServiceTestUtils.getDeviceId1();
        String deviceId2 = DeviceDedupServiceTestUtils.getDeviceId2();
        String deviceId3 = DeviceDedupServiceTestUtils.getDeviceId3();
        List<String> saveDeviceIds = List.of(deviceId1, deviceId2);
        List<String> notSaveDeviceIds = List.of(deviceId3);
        List<String> deviceIds = List.of(deviceId1, deviceId2, deviceId3);

        BDDMockito.given(redisLockService.tryLock(anyString()))
                .willReturn(true);
        BDDMockito.given(jdbcTemplate.queryForList(anyString(), eq(String.class), anyLong()))
                .willReturn(deviceIds);
        BDDMockito.given(redisPublishService.publishDeviceId(argThat(id -> id != null && saveDeviceIds.contains(id))))
                .willReturn(1L);
        BDDMockito.given(redisPublishService.publishDeviceId(argThat(id -> id != null && notSaveDeviceIds.contains(id))))
                .willReturn(0L);
        //when
        outboxScheduler.searchDeviceIdInTableDeviceOutbox();
        //then
        verify(redisLockService, times(1)).tryLock(anyString());
        verify(jdbcTemplate, times(1)).queryForList(anyString(), eq(String.class), anyLong());
        verify(redisPublishService, times(deviceIds.size())).publishDeviceId(anyString());
        verify(kafkaPublishService, times(saveDeviceIds.size())).publishDeviceId(anyString());
        verify(deviceOutboxRepository, times(deviceIds.size())).markSent(anyString());
        verify(deviceOutboxRepository, times(0)).markFailed(anyString(), anyString());
        verify(redisPublishService, times(0)).deleteDeviceId(anyString());
        verify(redisLockService, times(1)).unlock(anyString());

    }

    @Test
    @DisplayName("Kafka publish fails for one deviceId -> markFailed + delete from redis, others marked sent")
    public void givenKafkaPublishFailsForOneDeviceId_whenRunScheduler_thenMarkFailedAndDeleteFromRedisAndContinue() {
        //given
        String deviceId1 = DeviceDedupServiceTestUtils.getDeviceId1();
        String deviceId2 = DeviceDedupServiceTestUtils.getDeviceId2();
        String deviceId3 = DeviceDedupServiceTestUtils.getDeviceId3();
        List<String> saveDeviceIds = List.of(deviceId1, deviceId3);
        List<String> exceptionDeviceIds = List.of(deviceId2);
        List<String> deviceIds = List.of(deviceId1, deviceId2, deviceId3);

        BDDMockito.given(redisLockService.tryLock(anyString()))
                .willReturn(true);
        BDDMockito.given(jdbcTemplate.queryForList(anyString(), eq(String.class), anyLong()))
                .willReturn(deviceIds);
        BDDMockito.given(redisPublishService.publishDeviceId(argThat(id -> id != null && deviceIds.contains(id))))
                .willReturn(1L);

        BDDMockito.given(kafkaPublishService.publishDeviceId(argThat(id -> id != null && saveDeviceIds.contains(id))))
                .willReturn(true);

        BDDMockito.given(kafkaPublishService.publishDeviceId(argThat(id -> id != null && exceptionDeviceIds.contains(id))))
                .willThrow(new RuntimeException("Failed to publish device id to redis"));

        //when
        outboxScheduler.searchDeviceIdInTableDeviceOutbox();
        //then
        verify(redisLockService, times(1)).tryLock(anyString());
        verify(jdbcTemplate, times(1)).queryForList(anyString(), eq(String.class), anyLong());
        verify(redisPublishService, times(deviceIds.size())).publishDeviceId(anyString());
        verify(kafkaPublishService, times(deviceIds.size())).publishDeviceId(anyString());
        verify(deviceOutboxRepository, times(saveDeviceIds.size())).markSent(anyString());
        verify(deviceOutboxRepository, times(exceptionDeviceIds.size())).markFailed(anyString(), anyString());
        verify(redisPublishService, times(exceptionDeviceIds.size())).deleteDeviceId(anyString());
        verify(redisLockService, times(1)).unlock(anyString());

    }

    @Test
    @DisplayName("Try get lock and if lock is not acquired, skip processing the outbox")
    public void givenLockNotAcquired_whenRunScheduler_thenSkipProcessing() {
        //given
        BDDMockito.given(redisLockService.tryLock(anyString()))
                .willReturn(false);
        //when
        outboxScheduler.searchDeviceIdInTableDeviceOutbox();
        //then
        verify(redisLockService, times(1)).tryLock(anyString());
        verify(jdbcTemplate, times(0)).queryForList(anyString(), eq(String.class), anyLong());
        verify(redisPublishService, times(0)).publishDeviceId(anyString());
        verify(kafkaPublishService, times(0)).publishDeviceId(anyString());
        verify(deviceOutboxRepository, times(0)).markSent(anyString());
        verify(deviceOutboxRepository, times(0)).markFailed(anyString(), anyString());
        verify(redisPublishService, times(0)).deleteDeviceId(anyString());
        verify(redisLockService, times(0)).unlock(anyString());

    }
}
