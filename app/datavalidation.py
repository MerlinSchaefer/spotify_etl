import pandas as pd
from typing import List


def validate_played_data(df: pd.DataFrame, not_null_cols: List[str] = None) -> bool:
    """
    Validate the dataframe of recently played tracks.

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe of recently played tracks.

    Returns
    -------
    bool
        True if the dataframe is valid, False otherwise.
    """
    if not_null_cols is None:
        not_null_cols = ["played_at", "id"]
    # Check if dataframe is empty
    if df.empty:
        print("No tracks retrieved. Finishing execution")
        return False

    # Primary Key Check
    if not pd.Series(df["played_at"]).is_unique:
        raise PrimaryKeyError("Primary Key check is violated")

    # Check for nulls in non-nullable columns
    if df[not_null_cols].isnull().values.any():
        raise NullableError("Null values found in non-nullable columns")

    return True


def validate_audio_data(df: pd.DataFrame) -> bool:
    """
    Validate the dataframe of audio features.

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe of audio features.

    Returns
    -------
    bool
        True if the dataframe is valid, False otherwise.
    """

    # Check if dataframe is empty
    if df.empty:
        print("No tracks retrieved. Finishing execution")
        return False

    # Primary Key Check
    if not pd.Series(df["id"]).is_unique and not df["id"].isnull().values.any():
        raise PrimaryKeyError("Primary Key check is violated")

    return True


class PrimaryKeyError(Exception):
    """
    Exception raised for violation of the primary key constraint.
    """

    pass


class NullableError(Exception):
    """
    Exception raised for violation of the nullable constraint.
    """

    pass
