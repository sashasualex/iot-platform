package com.example.events_collector_service.redis;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PublishedDeviceIdsRedisService {
    private final StringRedisTemplate redisTemplate;
    @Value("${spring.redis.publish-key}")
    private String key;

    public Long publishDeviceId(String deviceId) {
        return redisTemplate.opsForSet().add(key, deviceId);
    }

    public void deleteDeviceId(String deviceId) {
        redisTemplate.opsForSet().remove(key, deviceId);
    }

}
