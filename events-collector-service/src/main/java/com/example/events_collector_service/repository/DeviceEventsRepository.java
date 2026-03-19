package com.example.events_collector_service.repository;

import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

@Repository
@RequiredArgsConstructor
public class DeviceEventsRepository {
    private final JdbcTemplate jdbc;

    public void saveEvent(GenericRecord event) {
        Long ts = Long.parseLong(event.get("timestamp").toString());
        LocalDate date = Instant.ofEpochMilli(ts).atZone(ZoneOffset.UTC).toLocalDate();
        String sql = """
                INSERT INTO events.device_events
                (device_id, event_id, event_date, timestamp_ms, type, payload) 
                VALUES 
                (?, ?, ?, ?, ?, ?)
                """;
        jdbc.update(sql,
                event.get("deviceId"),
                event.get("eventId"),
                java.sql.Date.valueOf(date),
                ts,
                event.get("type"),
                event.get("payload")
        );
    }

}

