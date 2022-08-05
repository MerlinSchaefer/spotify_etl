import pytest
import pandas as pd

# import datavalidation

## TODO create module structure for testing and validation function
## TODO use fixtures in a modular way
## TODO create unittests for validation functions

# @pytest.fixture
# def played_tracks_df_cols():
#     return [
#         "played_at",
#         "id",
#         "name",
#         "artists",
#         "album",
#         "duration_ms",
#         "explicit",
#         "href",
#         "is_local",
#         "popularity",
#         "uri",
#     ]


# @pytest.fixture
# def audio_features_df_cols():
#     return [
#         "id",
#         "danceability",
#         "energy",
#         "key",
#         "loudness",
#         "mode",
#         "speechiness",
#         "acousticness",
#         "instrumentalness",
#         "liveness",
#         "valence",
#         "tempo",
#         "analysis_url",
#         "time_signature",
#     ]


@pytest.fixture
def played_tracks_df():
    cols = [
        "album",
        "artists",
        "duration_ms",
        "explicit",
        "href",
        "id",
        "is_local",
        "name",
        "popularity",
        "uri",
        "played_at",
    ]
    values = [
        [
            "Tageslicht",
            "RAF Camora",
            173199,
            True,
            "https://api.spotify.com/v1/tracks/2c5CLmXwJ4V0IIqJ6B68gq",
            "2c5CLmXwJ4V0IIqJ6B68gq",
            False,
            "Tageslicht",
            56,
            "spotify:track:2c5CLmXwJ4V0IIqJ6B68gq",
            "2022-08-01 19:59:35.363000+00:00",
        ],
        [
            "Twelve Carat Toothache",
            "Post Malone",
            192840,
            False,
            "https://api.spotify.com/v1/tracks/0prbvDtiY8FrZKEj8LF4Rs",
            "0prbvDtiY8FrZKEj8LF4Rs",
            False,
            "I Like You (A Happier Song) (with Doja Cat)",
            70,
            "spotify:track:0prbvDtiY8FrZKEj8LF4Rs",
            "2022-08-01 19:56:41.087000+00:00",
        ],
        [
            "Un Verano Sin Ti",
            "Bad Bunny",
            178567,
            True,
            "https://api.spotify.com/v1/tracks/6Sq7ltF9Qa7SNFBsV5Cogx",
            "6Sq7ltF9Qa7SNFBsV5Cogx",
            False,
            "Me Porto Bonito",
            100,
            "spotify:track:6Sq7ltF9Qa7SNFBsV5Cogx",
            "2022-08-01 19:53:28.627000+00:00",
        ],
        [
            "Requiem",
            "Korn",
            219613,
            False,
            "https://api.spotify.com/v1/tracks/7aZsMvEdZBsZHUG3OGbPpD",
            "7aZsMvEdZBsZHUG3OGbPpD",
            False,
            "Let The Dark Do The Rest",
            54,
            "spotify:track:7aZsMvEdZBsZHUG3OGbPpD",
            "2022-08-01 19:45:59.816000+00:00",
        ],
        [
            "See What’s On The Inside",
            "Asking Alexandria",
            198041,
            True,
            "https://api.spotify.com/v1/tracks/0XHnYbXteyBDfVvk9EGaPu",
            "0XHnYbXteyBDfVvk9EGaPu",
            False,
            "Never Gonna Learn",
            59,
            "spotify:track:0XHnYbXteyBDfVvk9EGaPu",
            "2022-08-01 19:42:19.431000+00:00",
        ],
        [
            "Teardrops",
            "Bring Me The Horizon",
            215003,
            True,
            "https://api.spotify.com/v1/tracks/3aniWcwiiYKHpm3F5TdeKD",
            "3aniWcwiiYKHpm3F5TdeKD",
            False,
            "Teardrops",
            61,
            "spotify:track:3aniWcwiiYKHpm3F5TdeKD",
            "2022-08-01 19:39:01.091000+00:00",
        ],
        [
            "MANN MIT DER BRILLE",
            "2LADE",
            125560,
            False,
            "https://api.spotify.com/v1/tracks/00daiXpq7Jb76fXCJZA6rN",
            "00daiXpq7Jb76fXCJZA6rN",
            False,
            "Daywalker",
            44,
            "spotify:track:00daiXpq7Jb76fXCJZA6rN",
            "2022-08-01 19:35:25.938000+00:00",
        ],
        [
            "Put Your Hands Up",
            "ONEIL",
            141686,
            True,
            "https://api.spotify.com/v1/tracks/3WXOo12a4vYfRSNx1RzH0T",
            "3WXOo12a4vYfRSNx1RzH0T",
            False,
            "Put Your Hands Up",
            45,
            "spotify:track:3WXOo12a4vYfRSNx1RzH0T",
            "2022-08-01 16:17:19.972000+00:00",
        ],
    ]
    return pd.DataFrame(values, columns=cols)



