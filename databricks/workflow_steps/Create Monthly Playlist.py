# Databricks notebook source
from datetime import datetime, timedelta
from utils.authentication import authenticate, get_spotify_auth_vars


# COMMAND ----------

CLIENT_ID, CLIENT_SECRET, SCOPE, USERNAME = get_spotify_auth_vars( return_username=True)
# Add playlist creation to standard scope and authenticate
SCOPE = "user-read-recently-played user-read-currently-playing user-read-playback-state user-read-private playlist-modify-private playlist-read-private playlist-modify-public"
spotify = authenticate(CLIENT_ID, CLIENT_SECRET, SCOPE)


# COMMAND ----------

# Get last month
last_month = (datetime.now() - timedelta(days=30)).month
current_year = (datetime.now() - timedelta(days=30)).year

# COMMAND ----------

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


# COMMAND ----------

# create playlist
create_playlist_response = spotify.user_playlist_create(
    user=USERNAME,
    name=f"Flashback {current_year}-{str(last_month).zfill(2)}",
    public=False,
)
# retrieve playlist id
playlist_uri = create_playlist_response.get("id")

# COMMAND ----------

# add songs to playlist
spotify.user_playlist_add_tracks(
    user=USERNAME, playlist_id=playlist_uri, tracks=playlist_track_ids
)
print("Playlist created successfully.")
