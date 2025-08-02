import duckdb
import pandas as pd

df_old_tracks = pd.read_csv("databrickdump/as_csv/spotify_track_history.csv")
print(df_old_tracks.head())


connection = duckdb.connect(database="spotifydb.duckdb", read_only=False)
connection.execute(
    """
    CREATE SCHEMA IF NOT EXISTS spotify;
"""
)
connection.execute(
    """
    CREATE TABLE IF NOT EXISTS spotify.track_history (
    played_at TIMESTAMPTZ PRIMARY KEY,
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
"""
)
print("Database connection established.")
# Insert only new records into spotify.track_history
df_old_tracks["played_at"] = pd.to_datetime(df_old_tracks["played_at"])
played_tracks_df = df_old_tracks[
    [
        "played_at",
        "id",
        "name",
        "artists",
        "album",
        "duration_ms",
        "explicit",
        "href",
        "is_local",
        "popularity",
        "uri",
    ]
]
# connection.execute("""
#     INSERT INTO spotify.track_history
#     SELECT * FROM played_tracks_df;
# """)

print(connection.sql("SELECT COUNT(*) FROM spotify.track_history").df())
connection.close()
print("Data inserted successfully.")
