# Databricks notebook source
import logging
from etl.datacleaning import clean_recently_played, clean_audio_features
from etl.datavalidation import validate_played_data, validate_audio_data
from etl.upsert import upsert_df
from pydantic import ValidationError

# COMMAND ----------

# load raw data
recent_tracks = spark.read.format("delta").load("spotify.raw_track_history")

# COMMAND ----------

if validate_played_data(recent_tracks):
    # fetch audio features for the recently played tracks
    audio_features = spotify.audio_features(recent_tracks["id"].tolist())
    # clean the audio features and create dataframe
    # checking dataframe structure with pydantic
    try:
        audio_features_df = clean_audio_features(audio_features)
    except ValidationError as e:
        print(e)
# # validate the audio features dataframe
#         if validate_audio_data(audio_features_df):

#         if upsert_df(
#                 audio_features_df, "audio_features", "id", connection
#             ) and upsert_df(played_tracks_df, "track_history", "played_at", connection):
#                 print("Success")
#             else:
#                 print("Failed")

#         else:
#             print("Audio features dataframe is not valid.")
#     else:
#         print("Recently played tracks dataframe is not valid.")
