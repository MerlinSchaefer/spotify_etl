# Databricks notebook source
import logging

from utils.authentication import authenticate, get_spotify_auth_vars


# COMMAND ----------

client_id = dbutils.secrets.get(scope="spotify", key="SPOTIPY_CLIENT_ID")

# COMMAND ----------

CLIENT_ID, CLIENT_SECRET, SCOPE = get_spotify_auth_vars()

logging.info("Variables set")




# COMMAND ----------

# Authenticate with Spotify API
spotify = authenticate(CLIENT_ID, CLIENT_SECRET, SCOPE)


# COMMAND ----------

spotify.current_playback()

# COMMAND ----------

# retrieve recently played tracks
played_tracks = spotify.current_user_recently_played(limit=50)

# COMMAND ----------

# clean the recently played tracks and create dataframe
# checking dataframe structure with pydantic
try:
    played_tracks_df = clean_recently_played(played_tracks)
except ValidationError as e:
    print(e)
# validate the recently played tracks dataframe for sql requirements
played_tracks_df.display()
