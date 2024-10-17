# Databricks notebook source
from etl.upsert import upsert_df

# COMMAND ----------

# get staging tracks dataframe
tracks_df = spark.read.table("spotify.staging_track_history")
# get staging audio features dataframe
audio_features_df = spark.read.table("spotify.staging_audio_features")


# COMMAND ----------

upsert_df(audio_features_df, "audio_features", "id")
upsert_df(tracks_df, "track_history", "played_at")

