package com.nashkod.eventscollector.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public record AppProperties(
        Topics topics,
        Outbox outbox,
        Redis redis
) {
    public record Topics(String events, String devices) {}
    public record Outbox(int batchSize, long lockTtlMs, long cronDelayMs) {}
    public record Redis(String seenDevicesKey, String publishedDevicesKey, String outboxLockKey) {}
}
