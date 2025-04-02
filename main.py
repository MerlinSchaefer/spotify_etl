import duckdb
from app.configuration import set_spotify_variables
from app.authentication import authenticate
from app.datacleaning import clean_recently_played 
from app.datavalidation import validate_played_data
#from app.upsert import upsert_df # redo for duckdb
from pydantic import ValidationError
import pandas as pd

if __name__ == "__main__":
    CLIENT_ID, CLIENT_SECRET, SCOPE = set_spotify_variables()

    print("Variables set")

    # Authenticate with Spotify API
    spotify = authenticate(CLIENT_ID, CLIENT_SECRET, SCOPE)
    # retrieve recently played tracks
    played_tracks = spotify.current_user_recently_played(limit=50)
    # clean the recently played tracks and create dataframe
    # checking dataframe structure with pydantic
    try:
        played_tracks_df = clean_recently_played(played_tracks)
    except ValidationError as e:
        print(e)
    # validate the recently played tracks dataframe for sql requirements
    print(played_tracks_df.head())
    if validate_played_data(played_tracks_df):

        connection = duckdb.connect(database="spotifydb.duckdb", read_only=False)
        connection.execute("""
            CREATE SCHEMA IF NOT EXISTS spotify;
        """)
        connection.execute("""
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
        """)
        print("Database connection established.")
        # Insert only new records into spotify.track_history
        played_tracks_df['played_at'] = pd.to_datetime(played_tracks_df['played_at'])
        played_tracks_df =  played_tracks_df[["played_at", "id", "name", "artists", "album", "duration_ms", "explicit", "href", "is_local", "popularity", "uri"]]
	print(connection.sql("SELECT COUNT(*) FROM spotify.track_history").df())        
	connection.execute("""
            INSERT INTO spotify.track_history
            SELECT * FROM played_tracks_df
            WHERE played_at NOT IN (SELECT played_at FROM spotify.track_history);
        """)
        print(played_tracks_df.head())
        print(connection.sql("SELECT COUNT(*) FROM spotify.track_history").df())
        connection.close()
        print("Data inserted successfully.")

    else:
        print("Recently played tracks dataframe is not valid.")
