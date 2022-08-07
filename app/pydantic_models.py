import pandas as pd
from pydantic import BaseModel, Field
from pydantic.main import ModelMetaclass
from typing import Callable, List, Optional, Any, TypeVar


def validate_data_schema(data_schema: ModelMetaclass) -> Callable[..., Any]:
    """Validate a pd.DataFrame against the given data_schema. (decorator)

    Parameters
    ----------
    data_schema : ModelMetaclass
        The pydanctic data schema to validate the dataframe against.

    """

    def Inner(func: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            res = func(*args, **kwargs)
            if isinstance(res, pd.DataFrame):
                # check result of the function execution against the data_schema
                df_dict = res.to_dict(orient="records")

                # Wrap the data_schema into a helper class for validation
                class ValidationWrap(BaseModel):
                    df_dict: List[data_schema]  # type: ignore
                    # (ignoring the type of the data_schema as the way mypy and  pydantic work is not compatible here)

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
    id: str = Field(max_length=255)
    name: str = Field(max_length=255)
    artists: str = Field(max_length=255)
    album: str = Field(max_length=255)
    duration_ms: int
    explicit: bool
    href: str = Field(max_length=500)
    is_local: bool
    popularity: int = Field(ge=0, le=100)
    uri: str = Field(max_length=255)


class AudioFeaturesDataSchema(BaseModel):
    id: str = Field(max_length=255)
    danceability: float = Field(ge=0, le=1)
    energy: float = Field(ge=0, le=1)
    key: int = Field(ge=-1, le=11)
    loudness: float
    mode: int = Field(ge=0, le=1)
    speechiness: float = Field(ge=0, le=1)
    acousticness: float = Field(ge=0, le=1)
    instrumentalness: float = Field(ge=0, le=1)
    liveness: float
    valence: float = Field(ge=0, le=1)
    tempo: float = Field(ge=0)
    analysis_url: str = Field(max_length=500)
    time_signature: int = Field(ge=3, le=7)
