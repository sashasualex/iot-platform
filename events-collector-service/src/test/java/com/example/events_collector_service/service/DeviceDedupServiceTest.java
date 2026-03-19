package com.example.events_collector_service.service;

import com.example.events_collector_service.utils.DeviceDedupServiceTestUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.BDDMockito;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;

@ExtendWith(MockitoExtension.class)
public class DeviceDedupServiceTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private SetOperations<String, String> setOperations;

    @InjectMocks
    private DeviceDedupService deviceDedupService;
    private static final String REDIS_SET_KEY_TEST = "devices:seen";

    @Test
    @DisplayName("Test save device id to redis and return true if added successfully")
    public void givenDeviceId_whenAddDeviceIdAndReturnStatus_thenReturnTrue() {
        //given
        String deviceId = DeviceDedupServiceTestUtils.getRandomDeviceId();
        BDDMockito.given(redisTemplate.opsForSet())
                .willReturn(setOperations);
        BDDMockito.given(setOperations.add(eq(REDIS_SET_KEY_TEST), eq(deviceId)))
                .willReturn(1L);
        //when
        boolean result = deviceDedupService.addDeviceIdAndReturnStatus(deviceId);
        //then
        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("Test save device id to redis and return false if device id already exists")
    public void givenDeviceId_whenAddDeviceIdAndReturnStatus_thenReturnFalse() {
        //given
        String deviceId = DeviceDedupServiceTestUtils.getRandomDeviceId();
        BDDMockito.given(redisTemplate.opsForSet())
                .willReturn(setOperations);
        BDDMockito.given(setOperations.add(eq(REDIS_SET_KEY_TEST), eq(deviceId)))
                .willReturn(0L);
        //when
        boolean result = deviceDedupService.addDeviceIdAndReturnStatus(deviceId);
        //then
        assertThat(result).isFalse();
    }
}
