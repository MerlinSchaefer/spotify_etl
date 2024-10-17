import json
from datetime import datetime
from databricks.sdk.runtime import spark

def load_raw_data_to_table(raw_json: dict, table_name: str):
    """
    Loads the raw JSON response into a specified raw table.

    Example Usage for raw_track_history
        load_raw_data_to_table(response_json, "2024-09-14T10:30:00Z", "raw_track_history")

    Example Usage for raw_audio_features
    load_raw_data_to_table(response_json, "2024-09-14T10:35:00Z", "raw_audio_features")

    Args:
        raw_json (str): The raw JSON response from Spotify.
        table_name (str): The name of the raw table to load the data into 
                            (e.g., 'raw_track_history', 'raw_audio_features').


    """
    ingestion_time = datetime.utcnow()
    raw_json_str = json.dumps(raw_json)
    # Create a DataFrame with raw data and timestamp
    df = spark.createDataFrame([(ingestion_time, raw_json_str)], ["ingestion_time", "raw_data"])

    # Append the data to the specified raw table
    df.write.format("delta").mode("append").saveAsTable(f"spotify.{table_name}")


