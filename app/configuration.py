import configparser
from typing import List


def set_spotify_variables(return_username: bool = False) -> List[str]:
    """
    Set the variables for the spotify authentication.

    Returns
    -------
    list
        A list of the variables.
    """
    try:
        config = configparser.ConfigParser()
        config.read("app/config.ini")

        spotify_config = config["spotify"]

    except Exception as e:
        print(f"Error retrieving spotify configuration: {e}")

    # Spotify API authentication settings
    CLIENT_ID = spotify_config.get("client_id")
    CLIENT_SECRET = spotify_config.get("client_secret")
    SCOPE = "user-read-recently-played user-read-currently-playing user-read-playback-state user-read-private"
    if return_username:
        USERNAME = spotify_config.get("username")
        return [CLIENT_ID, CLIENT_SECRET, SCOPE, USERNAME]
    return [CLIENT_ID, CLIENT_SECRET, SCOPE]


def set_postgres_variables() -> List[str]:
    """
    Set the variables for the postgres authentication.

    Returns
    -------
    list
        A list of the variables.
    """
    try:
        config = configparser.ConfigParser()
        config.read("app/config.ini")

        postgres_config = config["postgres"]
    except Exception as e:
        print(f"Error retrieving postgres configuration: {e}")

    # DB settings
    DB_USER = postgres_config.get("user")
    DB_PASSWORD = postgres_config.get("password")
    DB_HOST = postgres_config.get("host")
    DB_NAME = postgres_config.get("dbname")
    PORT = postgres_config.get("port")
    REGION = postgres_config.get("region")
    return [DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, PORT, REGION]
