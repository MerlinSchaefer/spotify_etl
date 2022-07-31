import psycopg2
import os
import pandas as pd

db_user = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")


connection = psycopg2.connect(
    f"dbname={db_name} user={db_user} password={db_password} host={db_host} port=5432"
)

cursor = connection.cursor()

audio_features_table = """
    CREATE TABLE IF NOT EXISTS audio_features (
    id VARCHAR(255) PRIMARY KEY,
    danceability DOUBLE PRECISION, 
    energy DOUBLE PRECISION, 
    key INTEGER, 
    loudness DOUBLE PRECISION, 
    mode INTEGER, 
    speechiness DOUBLE PRECISION,
    acousticness DOUBLE PRECISION, 
    instrumentalness DOUBLE PRECISION, 
    liveness DOUBLE PRECISION, 
    valence DOUBLE PRECISION, 
    tempo DOUBLE PRECISION,
    analysis_url VARCHAR(500), 
    time_signature INTEGER
    );
"""

track_history_table = """
    CREATE TABLE IF NOT EXISTS track_history (
    played_at TIMESTAMP WITH TIME ZONE PRIMARY KEY,
    id VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    artists VARCHAR(255) ,
    album VARCHAR(255),
    duration_ms INTEGER,
    explicit BOOLEAN, 
    href VARCHAR(500),
    is_local BOOLEAN,
    popularity INTEGER,
    uri VARCHAR(255),
    CONSTRAINT fk_track_history_id 
        FOREIGN KEY(id) 
            REFERENCES audio_features(id)
    );
"""

audio_features_metadata = """
    CREATE TABLE IF NOT EXISTS audio_features_metadata (
    id SERIAL PRIMARY KEY,
    feature_name VARCHAR(255) NOT NULL,
    description VARCHAR(500) NOT NULL,
    type VARCHAR(255) NOT NULL,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION
    );
"""

cursor.execute(audio_features_table)
cursor.execute(track_history_table)
cursor.execute(audio_features_metadata)
connection.commit()

# check if tables were created successfully


table_check_query = """
SELECT "table_name","column_name", "data_type", "table_schema"
FROM INFORMATION_SCHEMA.COLUMNS
WHERE "table_schema" = 'public'
ORDER BY table_name  
"""
print(pd.read_sql(table_check_query, con=connection))

# load initial data


    
with open('initial_data/raw_audio_features.csv', 'r') as row:
    next(row) 
    cursor.copy_from(row, 'audio_features', sep=';', null='')

with open('initial_data/audio_feat_details.csv', 'r') as row:
    next(row)
    cursor.copy_from(row, 'audio_features_metadata', sep=';', null='')

with open('initial_data/raw_current_tracks.csv', 'r') as row:
    next(row)# Skip the header row.
    cursor.copy_from(row, 'track_history', sep=';',null='')
    
connection.commit()  