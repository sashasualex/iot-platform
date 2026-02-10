package com.nashkod.eventscollector.model;

import java.time.LocalDateTime;

public record OutboxRecord(String deviceId, LocalDateTime createdAt, int attempts) {
}
