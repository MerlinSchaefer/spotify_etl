import pytest
import pandas as pd
import numpy as np
from app.datavalidation import validate_audio_data, PrimaryKeyError, NullableError

# This file contains unit tests for the data validation function for played
# tracks in the datavalidation.py file.


@pytest.fixture
def audio_features_df():
    df_dict = {
        "danceability": {0: 0.774, 1: 0.736, 2: 0.911},
        "energy": {0: 0.558, 1: 0.681, 2: 0.712},
        "key": {0: 10, 1: 5, 2: 1},
        "loudness": {0: -8.915, 1: -6.063, 2: -5.105},
        "mode": {0: 0, 1: 1, 2: 0},
        "speechiness": {0: 0.0811, 1: 0.0745, 2: 0.0817},
        "acousticness": {0: 0.282, 1: 0.106, 2: 0.0901},
        "instrumentalness": {0: 0.383, 1: 0.0, 2: 2.68e-05},
        "liveness": {0: 0.151, 1: 0.12, 2: 0.0933},
        "valence": {0: 0.213, 1: 0.481, 2: 0.425},
        "tempo": {0: 122.041, 1: 100.924, 2: 92.005},
        "id": {
            0: "2c5CLmXwJ4V0IIqJ6B68gq",
            1: "0prbvDtiY8FrZKEj8LF4Rs",
            2: "6Sq7ltF9Qa7SNFBsV5Cogx",
        },
        "analysis_url": {
            0: "https://api.spotify.com/v1/audio-analysis/2c5CLmXwJ4V0IIqJ6B68gq",
            1: "https://api.spotify.com/v1/audio-analysis/0prbvDtiY8FrZKEj8LF4Rs",
            2: "https://api.spotify.com/v1/audio-analysis/6Sq7ltF9Qa7SNFBsV5Cogx",
        },
        "time_signature": {0: 4, 1: 4, 2: 4},
    }
    return pd.DataFrame(df_dict)


def test_validate_correct_data(audio_features_df):
    # test that the valid dataframe is detected
    assert validate_audio_data(audio_features_df) == True


def test_validate_empty_df():
    # test that an empty dataframe is detected
    assert validate_audio_data(pd.DataFrame()) == False



def test_validate_primary_key_unique(audio_features_df):
    # test that a dataframe with a duplicate primary key is detected
    audio_features_df.loc[0, "id"] = audio_features_df.loc[1, "id"]
    with pytest.raises(PrimaryKeyError):
        validate_audio_data(audio_features_df)

def test_validate_primary_key_null(audio_features_df):
    # test that a dataframe with a null primary key is detected
    audio_features_df.loc[0, "id"] = np.nan
    with pytest.raises(NullableError):
        validate_audio_data(audio_features_df)
