# Databricks notebook source
import logging

from utils.authentication import authenticate, get_spotify_auth_vars
from etl.data_loading import load_raw_data_to_table


# COMMAND ----------

CLIENT_ID, CLIENT_SECRET, SCOPE = get_spotify_auth_vars()


# COMMAND ----------

# Authenticate with Spotify API
spotify = authenticate(CLIENT_ID, CLIENT_SECRET, SCOPE)


# COMMAND ----------

# retrieve recently played tracks
played_tracks = spotify.current_user_recently_played(limit=50)

# COMMAND ----------

load_raw_data_to_table(raw_json= played_tracks, table_name="raw_track_history")
