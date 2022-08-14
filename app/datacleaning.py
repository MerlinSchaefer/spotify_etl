import re
import pandas as pd
from typing import Dict
from pydantic_models import (
    validate_data_schema,
    TracksDataSchema,
    AudioFeaturesDataSchema,
)


@validate_data_schema(data_schema=TracksDataSchema)  # type: ignore
def clean_recently_played(recently_played: Dict[str, str]) -> pd.DataFrame:
    """
    Load the recently played json dict into a pd.DataFrame
    and clean it to only contain necessary columns.

    Parameters
    ----------
    recently_played : dict
        The recently played tracks as the json dict from the API request.

    Returns
    -------
    df : pd.DataFrame
        The cleaned pd.DataFrame of recently played tracks.
    """
    df = pd.DataFrame(recently_played["items"])
    df = pd.concat(
        [
            df["track"].apply(pd.Series),  # the unnested track dict
            df["played_at"],  # the played_at column
        ],
        axis=1,
    )
    df["artists"] = df["artists"].apply(lambda x: x[0].get("name"))
    df["album"] = df["album"].apply(lambda x: x.get("name"))
    # drop unnecessary columns
    df = df.drop(
        [
            "available_markets",
            "disc_number",
            "external_ids",
            "external_urls",
            "preview_url",
            "track_number",
            "type",
        ],
        axis=1,
    )
    # remove apostrophes and quotation marks from the artists column
    df["artists"] = df["artists"].str.replace(r"[\"\',]", "", regex=True)
    # remove apostrophes and quotation marks from the album column
    df["album"] = df["album"].str.replace(r"[\"\',]", "", regex=True)
    # remove apostrophes and quotation marks from the name column
    df["name"] = df["name"].str.replace(r"[\"\',]", "", regex=True)
    return df


@validate_data_schema(data_schema=AudioFeaturesDataSchema)  # type: ignore
def clean_audio_features(audio_features: Dict[str, str]) -> pd.DataFrame:
    """
    Load the audio features json dict into a pd.DataFrame
    and clean the audio features dataframe.

    Parameters
    ----------
    audio_features : dict
        The audio features as the json dict from the API request.

    Returns
    -------
    df : pd.DataFrame
        The dataframe with audio features cleaned.
    """
    df = pd.DataFrame(audio_features)
    # drop duplicates as one track can be played multiple times
    df = df.drop_duplicates(subset="id")
    # drop unnecessary columns (mainly those already in the track dataframe)
    df = df.drop(["duration_ms", "uri", "track_href", "type"], axis=1)

    return df
