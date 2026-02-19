package com.example.events_collector_service.repository;

import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Date;

@Repository
@RequiredArgsConstructor
public class ClickhouseEvents {
    private final JdbcTemplate jdbc;

    public void saveEvent(GenericRecord event) {
        String sql = """
                INSERT INTO events.device_events
                (device_id, event_id, event_date, timestamp_ms, type, payload) 
                VALUES 
                (?, ?, ?, ?, ?, ?)
                """;
        jdbc.update(sql,
                event.get("deviceId"),
                event.get("eventId"),
                new Date(),
                event.get("timestamp"),
                event.get("type"),
                event.get("payload")
                );
    }

}

