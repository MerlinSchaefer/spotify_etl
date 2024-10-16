CREATE TABLE IF NOT EXISTS spotify.audio_features_metadata (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    feature_name STRING NOT NULL,
    description STRING NOT NULL,
    type STRING NOT NULL,
    min_value DOUBLE,
    max_value DOUBLE
);
