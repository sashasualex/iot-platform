package com.nashkod.eventscollector.service;

import com.nashkod.eventscollector.config.AppProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class RedisDedupService {

    private final StringRedisTemplate redisTemplate;
    private final AppProperties properties;

    public RedisDedupService(StringRedisTemplate redisTemplate, AppProperties properties) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
    }

    public boolean markSeenIfNew(String deviceId) {
        Long added = redisTemplate.opsForSet().add(properties.redis().seenDevicesKey(), deviceId);
        return added != null && added == 1;
    }

    public boolean markPublishedIfNew(String deviceId) {
        Long added = redisTemplate.opsForSet().add(properties.redis().publishedDevicesKey(), deviceId);
        return added != null && added == 1;
    }
}
