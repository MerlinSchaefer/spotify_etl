
def get_spotify_auth_vars(return_username: bool = False) -> list[str]:
    """
    Set the variables for the spotify authentication.

    Args:
        return_username (bool, optional): Whether to return the username. 
                                            Defaults to False.

    Returns:
        A list of the variables needed for spotify auth.
    """
    try:
        dbutils.secrets.get(scope="spotify-auth", key="client-id")
        dbutils.secrets.get(scope="spotify-auth", key="client-secret")
        

    except Exception as e:
        print(f"Error retrieving spotify configuration: {e}")

    # Spotify API authentication scope
    spotify_scope = "user-read-recently-played user-read-currently-playing user-read-playback-state user-read-private"
    if return_username:
        username = dbutils.secrets.get(scope="spotify-auth", key="username")
        return [client_id, client_secret, spotify_scope, username]
    return  [client_id, client_secret, spotify_scope]

