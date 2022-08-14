import os
import psycopg2
import os
import boto3
from authentication import authenticate
from datacleaning import clean_recently_played, clean_audio_features
from datavalidation import validate_played_data, validate_audio_data
from upsert import upsert_df
from pydantic import ValidationError

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
REGION = "eu-central-1"

if __name__ == "__main__":

    # Authenticate with Spotify API
    spotify = authenticate(CLIENT_ID, CLIENT_SECRET, SCOPE)
    # retrieve recently played tracks
    played_tracks = spotify.current_user_recently_played(limit=50)
    # clean the recently played tracks and create dataframe
    # checking dataframe structure with pydantic
    try:
        played_tracks_df = clean_recently_played(played_tracks)
    except ValidationError as e:
        print(e)
    # validate the recently played tracks dataframe for sql requirements
    print(played_tracks_df.head())
    if validate_played_data(played_tracks_df):
        # fetch audio features for the recently played tracks
        audio_features = spotify.audio_features(played_tracks_df["id"].tolist())
        # clean the audio features and create dataframe
        # checking dataframe structure with pydantic
        try:
            audio_features_df = clean_audio_features(audio_features)
        except ValidationError as e:
            print(e)
        print(audio_features_df.head())
        # validate the audio features dataframe
        if validate_audio_data(audio_features_df):

            # Authenticate with AWS and esstablish connection to database
            boto3_session = boto3.Session(profile_name="default")
            boto3_client = boto3_session.client("rds")
            token = boto3_client.generate_db_auth_token(
                DBHostname=DB_HOST, Port=PORT, DBUsername=DB_USER, Region=REGION
            )
            try:
                connection = psycopg2.connect(
                    f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={PORT}"
                )
                cursor = connection.cursor()
                query_results = cursor.execute("""SELECT now()""")
                query_results = cursor.fetchall()
                print(query_results)
                print("Database connection established.")
            except Exception as e:
                print(f"Database connection failed due to {e}")
            if upsert_df(
                audio_features_df, "audio_features", "id", connection
            ) and upsert_df(played_tracks_df, "track_history", "played_at", connection):
                print("Success")
                connection.commit()
            else:
                print("Failed")
            connection.close()
        else:
            print("Audio features dataframe is not valid.")
    else:
        print("Recently played tracks dataframe is not valid.")
