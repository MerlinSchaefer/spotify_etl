import pandas as pd
from pydantic import BaseModel, Field, conint, confloat, constr
from pydantic.main import ModelMetaclass
from typing import Callable, List, Optional


def validate_data_schema(data_schema: ModelMetaclass) -> Callable:
    """Validate a pd.DataFrame against the given data_schema. (decorator)

    Parameters
    ----------
    data_schema : ModelMetaclass
        The pydanctic data schema to validate the dataframe against.

    """

    def Inner(func):
        def wrapper(*args, **kwargs):
            res = func(*args, **kwargs)
            if isinstance(res, pd.DataFrame):
                # check result of the function execution against the data_schema
                df_dict = res.to_dict(orient="records")

                # Wrap the data_schema into a helper class for validation
                class ValidationWrap(BaseModel):
                    df_dict: List[data_schema]

                # Do the validation
                _ = ValidationWrap(df_dict=df_dict)
            else:
                raise TypeError(
                    "Your Function is not returning an object of type pandas.DataFrame."
                )

            # return the function result
            return res

        return wrapper

    return Inner


class TracksDataSchema(BaseModel):
    played_at: pd.datetime64
    id: constr = Field(max_length=255)
    name: constr = Field(max_length=255)
    artists: constr = Field(max_length=255)
    album: constr = Field(max_length=255)
    duration_ms: conint
    explicit: bool
    href: constr = Field(max_length=500)
    is_local: bool
    popularity: conint = Field(ge=0, le=100)
    uri: constr = Field(max_length=255)


class AudioFeaturesDataSchema(BaseModel):
    id: constr = Field(max_length=255)
    danceability: confloat = Field(ge=0, le=1)
    energy: confloat = Field(ge=0, le=1)
    key: conint = Field(ge=-1, le=11)
    loudness: confloat
    mode: conint = Field(ge=0, le=1)
    speechiness: confloat = Field(ge=0, le=1)
    acousticness: confloat = Field(ge=0, le=1)
    instrumentalness: confloat = Field(ge=0, le=1)
    liveness: confloat
    valence: confloat = Field(ge=0, le=1)
    tempo: confloat = Field(ge=0)
    analysis_url: constr = Field(max_length=500)
    time_signature: conint = Field(ge=3, le=7)
