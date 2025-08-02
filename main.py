import os
import time
import dotenv
import duckdb
from app.configuration import set_spotify_variables
from app.authentication import authenticate
from app.datacleaning import clean_recently_played 
from app.datavalidation import validate_played_data
from pydantic import ValidationError
import pandas as pd
from pathlib import Path
from app.grafana_logger import JsonGrafanaLogger, EventType
import logging

BASE_DIR = Path(__file__).resolve().parent
LOG_PATH = BASE_DIR / "logs" / "grafana_events.json"

# Initialize the loggers
# standard logger for general logging
logger = logging.getLogger("spotify_etl")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Grafana logger for structured logging of main script events
grafana_logger = JsonGrafanaLogger("grafana", LOG_PATH)

if __name__ == "__main__":
    start_time = time.time()  # Start time for the script
    CLIENT_ID, CLIENT_SECRET, SCOPE = set_spotify_variables()
    dotenv.load_dotenv(dotenv_path=dotenv.find_dotenv())
    logger.info("Variables set")
    cache_path = os.getenv("CACHE_PATH")
    logger.debug(f"Cache path: {cache_path}")
    db_path = os.getenv("DB_PATH")
    logger.debug(f"Database path: {db_path}")
    # Authenticate with Spotify API
    try:
        spotify = authenticate(CLIENT_ID, CLIENT_SECRET, SCOPE, cache_path=cache_path)
    except Exception as e:
        logger.error(f"Error authenticating with Spotify API: {e}")
        grafana_logger.log_event(EventType.ERROR, "Auth failure", 0, "Error authenticating with Spotify API", {"error": str(e)})
        raise e
    logger.debug("Authenticated with Spotify API")
    # retrieve recently played tracks
    played_tracks = spotify.current_user_recently_played(limit=50)
    # clean the recently played tracks and create dataframe
    # checking dataframe structure with pydantic
    try:
        played_tracks_df = clean_recently_played(played_tracks)
    except ValidationError as e:
        logger.error(f"Validation error in recently played tracks: {e}")
        grafana_logger.log_event(EventType.ERROR, "Data validation failure", 0, "Validation error in recently played tracks", {"error": str(e)})
        raise e
    # validate the recently played tracks dataframe for sql requirements
    if validate_played_data(played_tracks_df):

        connection = duckdb.connect(database=db_path, read_only=False)
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
        logger.info("Database connection established.")
        # Insert only new records into spotify.track_history
        played_tracks_df['played_at'] = pd.to_datetime(played_tracks_df['played_at'])
        played_tracks_df =  played_tracks_df[["played_at", "id", "name", "artists", "album", "duration_ms", "explicit", "href", "is_local", "popularity", "uri"]]
        current_num_tracks = connection.sql("SELECT COUNT(*) FROM spotify.track_history").df().iloc[0, 0]
        logger.info(f"Current number of tracks in database: {current_num_tracks}")
        connection.execute("""
            INSERT INTO spotify.track_history
            SELECT * FROM played_tracks_df
            WHERE played_at NOT IN (SELECT played_at FROM spotify.track_history);
        """)
        logger.info(f"Inserted {played_tracks_df.shape[0]} new tracks into database.")
        print(connection.sql("SELECT COUNT(*) FROM spotify.track_history").df())
        connection.close()
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Data inserted successfully. Script completed in {duration:.2f} seconds.")
        grafana_logger.log_event(event_type= EventType.TRACKS_ADDED, 
                                 status="success",
                                 duration_s=duration,
                                message="Successfully added new tracks to the database",
                                metadata={"new_tracks_count": int(played_tracks_df.shape[0]), # for json serialization
                                  "total_tracks": int(current_num_tracks + played_tracks_df.shape[0])})

    else:
        logger.error("Recently played tracks dataframe is not valid.")
        grafana_logger.log_event(EventType.ERROR, "Data validation failure", 0,
                                "Recently played tracks dataframe is not valid")
        raise ValueError("Recently played tracks dataframe is not valid.")
