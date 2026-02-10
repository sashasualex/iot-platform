package com.nashkod.eventscollector.service;

import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

@Service
public class RedisLockService {

    private static final String RELEASE_SCRIPT = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

    private final StringRedisTemplate redisTemplate;

    public RedisLockService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public boolean tryLock(String key, String token, Duration ttl) {
        Boolean locked = redisTemplate.opsForValue().setIfAbsent(key, token, ttl);
        return Boolean.TRUE.equals(locked);
    }

    public void unlock(String key, String token) {
        redisTemplate.execute(
                connection -> connection.scriptingCommands().eval(
                        RELEASE_SCRIPT.getBytes(StandardCharsets.UTF_8),
                        ReturnType.INTEGER,
                        1,
                        key.getBytes(StandardCharsets.UTF_8),
                        token.getBytes(StandardCharsets.UTF_8)
                ),
                true
        );
    }
}
