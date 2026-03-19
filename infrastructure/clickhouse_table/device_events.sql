CREATE TABLE IF NOT EXISTS events.device_events
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
    ORDER BY (device_id, event_date, timestamp_ms, event_id);