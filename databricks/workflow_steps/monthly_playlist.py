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

    spotify = authenticate(CLIENT_ID, CLIENT_SECRET, SCOPE)
    print("Authenticated with Spotify API")
    # query track database

