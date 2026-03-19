package com.example.events_collector_service.redis;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;

@Service
@RequiredArgsConstructor
public class RedisLockService {
    private final StringRedisTemplate redisTemplate;
    @Value("${spring.redis.lock-time-in-seconds}")
    private long timeToLock;
    @Value("${spring.redis.lock-key}")
    private String key;


    public Boolean tryLock(String token) {
        return redisTemplate.opsForValue().setIfAbsent(key, token, Duration.ofSeconds(timeToLock));
    }

    public void unlock(String token) {
        if (token.equals(redisTemplate.opsForValue().get(key))) {
            redisTemplate.delete(key);
        }
    }
}
