package com.example.events_collector_service.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DeviceDedupService {

    private final RedisTemplate<String,String> redisTemplate;
    private static final String REDIS_SET_KEY = "devices:seen";

    public boolean addDeviceIdAndReturnStatus(String deviceId) {
        Long added = redisTemplate.opsForSet().add(REDIS_SET_KEY, deviceId);
        return added != null && added == 1L;
    }


}
