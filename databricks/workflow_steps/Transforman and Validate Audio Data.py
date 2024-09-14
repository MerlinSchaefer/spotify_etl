# Databricks notebook source
from etl.data_cleaning import clean_audio_features
from etl.data_validation import is_audio_data_valid
from pydantic import ValidationError
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# COMMAND ----------

STAGING_TABLE = "staging_audio_features"

# COMMAND ----------

# Fetch the latest record from raw_track_history
latest_raw_json = spark.sql("""
    SELECT raw_data, ingestion_time 
    FROM spotify.raw_audio_features
    ORDER BY ingestion_time DESC 
    LIMIT 1
""").collect()[0]['raw_data']

# COMMAND ----------

try:
    # try to clean and transform data into valid pydantic model
    transformed_df  = clean_audio_features(latest_raw_json)
except ValidationError as e:
        print(e)


# COMMAND ----------

if is_audio_data_valid(transformed_df):
    transformed_df = (spark.createDataFrame(transformed_df)
                           .withColumn("key", col("key").cast(IntegerType()))
                           .withColumn("mode", col("mode").cast(IntegerType()))
                           .withColumn("time_signature", col("time_signature").cast(IntegerType()))
                      )
    # Overwrite the staging table with transformed data
    (transformed_df
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(f"spotify.{STAGING_TABLE}")
    )
