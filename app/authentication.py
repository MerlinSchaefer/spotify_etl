import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os


CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")

scope = "user-read-recently-played user-read-currently-playing user-read-playback-state user-read-private"


def authenticate(
    client_id: str,
    client_secret: str,
    scope: str,
    redirect_uri: str = "http://localhost:8000",
) -> spotipy.Spotify:
    """
    Authenticate with Spotify via client id and client secret.

    Parameters
    ----------
    client_id : str
        The client id provided by Spotify.
    client_secret : str
        The client secret provided by Spotify.
    scope : str
        The scope of the authentication (e.g. "user-read-currently-played").
    redirect_uri : str, optional
        The redirect uri provided set for the spotify application in the dashboard.
        The default is "http://localhost:8000".
    """
    return spotipy.Spotify(
        auth_manager=SpotifyOAuth(
            scope=scope,
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            cache_path=".cache",
        )
    )
