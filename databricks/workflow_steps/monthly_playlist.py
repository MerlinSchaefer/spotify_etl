import os
import pandas as pd
from datetime import datetime, timedelta
from app.configuration import set_spotify_variables, set_postgres_variables
from app.authentication import authenticate

if __name__ == "__main__":
    # set credentials
    CLIENT_ID, CLIENT_SECRET, SCOPE, USERNAME = set_spotify_variables(
        return_username=True
    )
    # Get last month
    last_month = (datetime.now() - timedelta(days=30)).month
    current_year = (datetime.now() - timedelta(days=30)).year
    print(f"Creating Playlist 'Flashback {current_year}-{last_month}'")
    # Add playlist creation to standard scope and authenticate
    SCOPE = SCOPE + " playlist-modify-private playlist-read-private"
    spotify = authenticate(CLIENT_ID, CLIENT_SECRET, SCOPE)
    print("Authenticated with Spotify API")
    # query track database

    last_month_track_history_query = f"""
        SELECT * 
        FROM spotify.track_history AS tracks
            LEFT JOIN spotify.audio_features AS audio 
                ON tracks.id = audio.id
        WHERE EXTRACT(MONTH from played_at) = {last_month};
        """
    last_month_tracks_df = spark.sql(last_month_track_history_query).toPandas()
    track_names = last_month_tracks_df["name"].value_counts()
    recurring_tracks = track_names[(last_month_tracks_df["name"].value_counts() >= 3)]
    reccuring_tracks_df = last_month_tracks_df[
        last_month_tracks_df["name"].isin(recurring_tracks.index)
    ]
    # get all unique ids for playlist generation
    playlist_track_ids = reccuring_tracks_df.iloc[:, 1].unique().tolist()
    # create playlist
    create_playlist_response = spotify.user_playlist_create(
        user=USERNAME,
        name=f"Flashback {current_year}-{last_month}",
        public=False,
        collaborative=False,
        description="""Monthly flashback playlist based on tracks 
        from track history db that have been listened to at least 3 times.""",
    )
    # retrieve playlist id
    playlist_uri = create_playlist_response.get("id")
    # add songs to playlist
    spotify.user_playlist_add_tracks(
        user=USERNAME, playlist_id=playlist_uri, tracks=playlist_track_ids
    )
    print("Playlist created successfully.")
