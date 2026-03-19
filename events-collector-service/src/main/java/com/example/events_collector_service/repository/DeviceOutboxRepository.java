package com.example.events_collector_service.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

@Repository
@RequiredArgsConstructor
public class DeviceOutboxRepository {
    private final JdbcTemplate jdbcTemplate;

    public void saveOutbox(String deviceId) {
        String sql = """
                INSERT INTO events.device_outbox 
                (device_id, created_at, status, sent_at, attempts, last_error) 
                VALUES 
                (?, ?, ?, ?, ?, ?)
                """;
        jdbcTemplate.update(sql,
                deviceId,
                java.sql.Timestamp.valueOf(LocalDateTime.now().withNano(0)),
                0,
                java.sql.Timestamp.valueOf(LocalDateTime.now().withNano(0)),
                0,
                ""
        );
    }

    public void markSent(String deviceId) {
        String sql = """
                ALTER TABLE events.device_outbox 
                UPDATE status = 1, attempts = attempts + 1, sent_at = ?
                WHERE device_id = ? AND status = 0
                """;
        jdbcTemplate.update(sql, java.sql.Timestamp.valueOf(LocalDateTime.now().withNano(0)), deviceId);
    }

    public void markFailed(String deviceId, String errorMessage) {
        String sql = """
                ALTER TABLE events.device_outbox 
                UPDATE attempts = attempts + 1, last_error = ?, sent_at = ?
                WHERE device_id = ? AND status = 0
                """;
        jdbcTemplate.update(sql, errorMessage, java.sql.Timestamp.valueOf(LocalDateTime.now().withNano(0)), deviceId);
    }
}
