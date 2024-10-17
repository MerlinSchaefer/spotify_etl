# Databricks notebook source
from utils.authentication import authenticate, get_spotify_auth_vars
from etl.data_loading import load_raw_data_to_table

# COMMAND ----------

# get current track_history data
played_tracks_df = spark.table("spotify.staging_track_history").toPandas()

# COMMAND ----------

CLIENT_ID, CLIENT_SECRET, SCOPE = get_spotify_auth_vars()

# COMMAND ----------

# Authenticate with Spotify API
spotify = authenticate(CLIENT_ID, CLIENT_SECRET, SCOPE)

# COMMAND ----------

audio_features = spotify.audio_features(played_tracks_df["id"].tolist())

# COMMAND ----------

load_raw_data_to_table(raw_json= audio_features, table_name="raw_audio_features")
