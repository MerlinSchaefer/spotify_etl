import psycopg2
import os
import pandas as pd
from datetime import datetime
from app.configuration import set_spotify_variables, set_postgres_variables
from app.authentication import authenticate

if __name__ == "__main__":
    # set credentials
    CLIENT_ID, CLIENT_SECRET, SCOPE, USERNAME = set_spotify_variables(
        return_username=True
    )
    DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, PORT, REGION = set_postgres_variables()
    print("Variables set")
    # Get last month
    last_month = datetime.now().month - 1
    current_year = datetime.now().year
    print(f"Creating Playlist 'Flashback {current_year}-{last_month}'")
    # Authenticate with Spotify API
    SCOPE = SCOPE + " playlist-modify-private playlist-read-private"
    spotify = authenticate(CLIENT_ID, CLIENT_SECRET, SCOPE)
    print("Authenticated with Spotify API")
    # query track database
    connection = psycopg2.connect(
        f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={PORT}"
    )

    cursor = connection.cursor()
    last_month_track_history_query = f"""
        SELECT * 
        FROM track_history 
            LEFT JOIN audio_features ON track_history.id = audio_features.id
        WHERE EXTRACT(MONTH from played_at) = {last_month};
        """
    last_month_tracks_df = pd.read_sql(last_month_track_history_query, con=connection)
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
        from track history db that have been listened to at least twice.""",
    )
    # retrieve playlist id
    playlist_uri = create_playlist_response.get("id")
    # add songs to playlist
    spotify.user_playlist_add_tracks(
        user=USERNAME, playlist_id=playlist_uri, tracks=playlist_track_ids
    )
    print("Playlist created successfully.")
