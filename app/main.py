import pandas as pd
import os
import psycopg2
from authentication import authenticate
from datacleaning import clean_recently_played, clean_audio_features
from datavalidation import validate_played_data, validate_audio_data

# Spotify API authentication settings
CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
SCOPE = "user-read-recently-played user-read-currently-playing user-read-playback-state user-read-private"

# DB settings
DB_USER = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
PORT = "5432"

DEBUG = True

if __name__ == "__main__":

    # Authenticate with Spotify API
    spotify = authenticate(CLIENT_ID, CLIENT_SECRET, SCOPE)
    # retrieve recently played tracks
    played_tracks = spotify.current_user_recently_played(limit=50)
    # clean the recently played tracks and create dataframe
    played_tracks_df = clean_recently_played(played_tracks)
    # validate the recently played tracks dataframe
    if validate_played_data(played_tracks_df):
        # fetch audio features for the recently played tracks
        audio_features = spotify.audio_features(played_tracks_df["id"].tolist())
        # clean the audio features and create dataframe
        audio_features_df = clean_audio_features(audio_features)
        # validate the audio features dataframe
        if validate_audio_data(audio_features_df):
            if DEBUG:
                played_tracks_df.to_csv("played_tracks.csv", index=False, sep=";")
                audio_features_df.to_csv("audio_features.csv", index=False, sep=";")
            else:
                # connect to the database
                connection = psycopg2.connect(f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={PORT}")
                # create a cursor object
                cursor = connection.cursor()
                # insert the recently played tracks dataframe into the database
                played_tracks_df.to_sql(name="played_tracks", con=connection, if_exists="append", index=False)
                # insert the audio features dataframe into the database
                audio_features_df.to_sql(name="audio_features", con=connection, if_exists="append", index=False)
                # commit the changes to the database
                connection.commit()
                # close the connection
                connection.close()
        else:
            print("Audio features dataframe is not valid.")
    else:
        print("Recently played tracks dataframe is not valid.")
        
