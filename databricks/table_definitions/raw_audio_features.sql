CREATE TABLE IF NOT EXISTS spotify.raw_audio_features (
    raw_data STRING,  -- This stores the entire JSON response as a string
    ingestion_time TIMESTAMP  -- Tracks when the data was ingested
    );