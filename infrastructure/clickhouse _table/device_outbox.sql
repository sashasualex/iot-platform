CREATE TABLE IF NOT EXISTS events.device_outbox
(
    device_id     String,
    created_at    DateTime,
    status        UInt8,          -- 0 = NEW, 1 = SENT
    sent_at       DateTime,
    attempts      UInt32,
    last_error    String
)
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(created_at)
    ORDER BY (status, created_at, device_id);