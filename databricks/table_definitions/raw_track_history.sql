CREATE TABLE IF NOT EXISTS spotify.raw_track_history (
    played_at TIMESTAMP,
    id STRING NOT NULL,
    name STRING,
    artists STRING,
    album STRING,
    duration_ms INT,
    explicit BOOLEAN, 
    href STRING,
    is_local BOOLEAN,
    popularity INT,
    uri STRING
);
