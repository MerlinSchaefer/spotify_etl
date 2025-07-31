import duckdb
import argparse
import os
import time
import dotenv
import pandas as pd
import logging
from datetime import datetime, timedelta
from app.configuration import set_spotify_variables
from app.authentication import authenticate
from app.grafana_logger import JsonGrafanaLogger, EventType


if __name__ == "__main__":
    logger = logging.getLogger("monthly_playlist")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    grafana_logger = JsonGrafanaLogger('grafana', 'logs/grafana_events.json')

    parser = argparse.ArgumentParser(description="Generate monthly Spotify playlist.")
    parser.add_argument("--month", type=int, help="Month number (1-12) to generate playlist for. Defaults to last month.", default=None)
    parser.add_argument("--year", type=int, help="Year to generate playlist for. Defaults to current year if not specified.", default=None)
    args = parser.parse_args()

    start_time = time.time()  

    dotenv.load_dotenv(dotenv_path=dotenv.find_dotenv())
    CLIENT_ID, CLIENT_SECRET, SCOPE, USERNAME = set_spotify_variables(return_username=True)
    cache_path = os.getenv("CACHE_PATH")
    db_path = os.getenv("DB_PATH")
    logger.info("Variables set")

    # Determine month and year
    if args.month is not None:
        target_month = args.month
        target_year = args.year if args.year is not None else datetime.now().year
    else:
        last_month_date = datetime.now() - timedelta(days=28)
        target_month = last_month_date.month
        target_year = last_month_date.year

    logger.info(f"Creating Playlist 'Flashback {target_year}-{target_month}'")
    # Authenticate with Spotify API
    SCOPE = SCOPE + " playlist-modify-private playlist-read-private"
    try:
        spotify = authenticate(CLIENT_ID, CLIENT_SECRET, SCOPE)
        logger.info("Authenticated with Spotify API")
    except Exception as e:
        logger.error(f"Error authenticating with Spotify API: {e}")
        grafana_logger.log_event(EventType.ERROR, "Auth failure", 0, "Error authenticating with Spotify API", {"error": str(e)})
        raise e

    # query track database
    connection = duckdb.connect(database=db_path, read_only=False)

    last_month_track_history_query = f"""
        SELECT * 
        FROM spotify.track_history 
        WHERE EXTRACT(MONTH from played_at) = {target_month} AND EXTRACT(YEAR from played_at) = {target_year};
        """
    last_month_tracks_df = pd.read_sql(last_month_track_history_query, con=connection)
    track_names = last_month_tracks_df["name"].value_counts()
    recurring_tracks = track_names[(last_month_tracks_df["name"].value_counts() >= 3)]
    reccuring_tracks_df = last_month_tracks_df[
        last_month_tracks_df["name"].isin(recurring_tracks.index)
    ]
    # get all unique ids for playlist generation
    playlist_track_ids = reccuring_tracks_df.iloc[:, 1].unique().tolist()
    logger.info(f"Found {len(playlist_track_ids)} unique track IDs for playlist.")

    # create playlist
    create_playlist_response = spotify.user_playlist_create(
        user=USERNAME,
        name=f"Flashback {target_year}-{target_month}",
        public=False,
        collaborative=False,
    )
    if not create_playlist_response:
        logger.error("Failed to create playlist.")
        grafana_logger.log_event(EventType.ERROR, "Playlist creation failure", 0, "Failed to create playlist")
        raise Exception("Failed to create playlist")
    playlist_uri = create_playlist_response.get("id")

    logger.info(f"Playlist created with ID: {playlist_uri}")
    grafana_logger.log_event(
        EventType.PLAYLIST_CREATED, "success", time.time() - start_time,
        "Successfully created playlist",
        {"playlist_id": playlist_uri, "month": target_month, "year": target_year}
    )
    # retrieve playlist id

    # add songs to playlist in batches of 100
    max_batch_size = 100
    for i in range(0, len(playlist_track_ids), max_batch_size):
        batch = playlist_track_ids[i:i + max_batch_size]
        spotify.user_playlist_add_tracks(
            user=USERNAME, playlist_id=playlist_uri, tracks=batch
        )
        logger.info(f"Added tracks {i+1} to {i+len(batch)} to playlist.")
    grafana_logger.log_event(
        EventType.PLAYLIST_UPDATED, "success", time.time() - start_time,
        "Successfully updated playlist with new tracks",
        {"playlist_id": playlist_uri, "num_added_tracks": len(playlist_track_ids), "added_tracks": playlist_track_ids}
    )
    logger.info("Playlist created successfully.")
