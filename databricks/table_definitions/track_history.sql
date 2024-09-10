CREATE TABLE IF NOT EXISTS track_history (
    played_at TIMESTAMP PRIMARY KEY,
    id STRING NOT NULL,
    name STRING,
    artists STRING,
    album STRING,
    duration_ms INT,
    explicit BOOLEAN, 
    href STRING,
    is_local BOOLEAN,
    popularity INT,
    uri STRING,
    CONSTRAINT fk_track_history_id 
        FOREIGN KEY (id) 
            REFERENCES audio_features(id)
);
