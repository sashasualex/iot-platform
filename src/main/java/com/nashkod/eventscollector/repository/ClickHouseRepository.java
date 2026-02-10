package com.nashkod.eventscollector.repository;

import com.nashkod.avro.DeviceEvent;
import com.nashkod.eventscollector.model.OutboxRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

@Repository
public class ClickHouseRepository {

    private final JdbcTemplate jdbcTemplate;

    public ClickHouseRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void initSchema() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS device_events
                (
                    device_id     String,
                    event_id      String,
                    event_date    Date,
                    timestamp_ms  Int64,
                    type          String,
                    payload       String
                )
                ENGINE = MergeTree
                PARTITION BY event_date
                ORDER BY (device_id, event_date, timestamp_ms, event_id)
                """);

        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS device_outbox
                (
                    device_id     String,
                    created_at    DateTime,
                    status        UInt8,
                    sent_at       DateTime,
                    attempts      UInt32,
                    last_error    String
                )
                ENGINE = MergeTree
                PARTITION BY toYYYYMM(created_at)
                ORDER BY (status, created_at, device_id)
                """);
    }

    public void insertEvent(DeviceEvent event) {
        LocalDate eventDate = Instant.ofEpochMilli(event.getTimestamp()).atZone(ZoneOffset.UTC).toLocalDate();
        jdbcTemplate.update(
                "INSERT INTO device_events (device_id, event_id, event_date, timestamp_ms, type, payload) VALUES (?,?,?,?,?,?)",
                event.getDeviceId().toString(),
                event.getEventId().toString(),
                eventDate,
                event.getTimestamp(),
                event.getType().toString(),
                event.getPayload().toString()
        );
    }

    public void insertOutboxNew(String deviceId) {
        jdbcTemplate.update(
                "INSERT INTO device_outbox (device_id, created_at, status, sent_at, attempts, last_error) VALUES (?, now(), 0, toDateTime(0), 0, '')",
                deviceId
        );
    }

    public List<OutboxRecord> findNewOutboxRecords(int limit) {
        return jdbcTemplate.query(
                "SELECT device_id, created_at, attempts FROM device_outbox WHERE status=0 ORDER BY created_at LIMIT ?",
                (rs, rowNum) -> new OutboxRecord(
                        rs.getString("device_id"),
                        rs.getTimestamp("created_at").toLocalDateTime(),
                        rs.getInt("attempts")
                ),
                limit
        );
    }

    public void markSent(OutboxRecord record) {
        jdbcTemplate.execute("ALTER TABLE device_outbox UPDATE status=1, sent_at=now(), last_error='' WHERE device_id='" +
                record.deviceId() + "' AND created_at=toDateTime('" + record.createdAt() + "') AND status=0");
    }

    public void markFailed(OutboxRecord record, String error) {
        String escapedError = error.replace("'", "\\'");
        jdbcTemplate.execute("ALTER TABLE device_outbox UPDATE attempts=attempts+1, last_error='" + escapedError +
                "' WHERE device_id='" + record.deviceId() + "' AND created_at=toDateTime('" + record.createdAt() + "') AND status=0");
    }

    public long pendingOutboxCount() {
        Long count = jdbcTemplate.queryForObject("SELECT count() FROM device_outbox WHERE status=0", Long.class);
        return count == null ? 0 : count;
    }
}
