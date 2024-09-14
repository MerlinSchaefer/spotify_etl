import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
from databricks.sdk.runtime import *
from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)

def get_spotify_auth_vars(return_username: bool = False) -> list[str]:
    """
    Set the variables for the spotify authentication.

    Args:
        return_username (bool, optional): Whether to return the username. 
                                            Defaults to False.

    Returns:
        A list of the variables needed for spotify auth.
    """
     # Spotify API authentication scope
    spotify_scope = "user-read-recently-played user-read-currently-playing user-read-playback-state user-read-private"
    
    try:
        client_id = dbutils.secrets.get(scope="spotify", key="SPOTIPY_CLIENT_ID")
        client_secret = dbutils.secrets.get(scope="spotify", key="SPOTIPY_CLIENT_SECRET")
        

    except Exception as e:
        print(f"Error retrieving spotify configuration: {e}")

    if return_username:
        username = dbutils.secrets.get(scope="spotify", key="username")
        return [client_id, client_secret, spotify_scope, username]
    return  [client_id, client_secret, spotify_scope]


def authenticate(
    client_id: str,
    client_secret: str,
    scope: str,
    redirect_uri: str = "http://localhost:8888",
) -> spotipy.Spotify:
    """
    Authenticate with Spotify via client id and client secret in Databricks.

    Parameters
    ----------
    client_id : str
        The client id provided by Spotify.
    client_secret : str
        The client secret provided by Spotify.
    scope : str
        The scope of the authentication (e.g. "user-read-currently-played").
    redirect_uri : str, optional
        The redirect uri set for the Spotify application in the dashboard.
        The default is "http://localhost:8888".
    """
    # Define a persistent path on DBFS for storing the cache
    cache_path = "/dbfs/tmp/spotify/.cache"

    # Ensure the DBFS path exists
    os.makedirs("/dbfs/tmp/spotify", exist_ok=True)

    auth_manager = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        scope=scope,
        cache_path="/dbfs/tmp/spotify/.cache",
        open_browser=False
    )
    
   
    return spotipy.Spotify(auth_manager=auth_manager)

