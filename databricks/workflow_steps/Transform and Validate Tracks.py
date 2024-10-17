# Databricks notebook source
from etl.data_cleaning import clean_recently_played
from etl.data_validation import is_played_data_valid
from pydantic import ValidationError
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType


# COMMAND ----------

STAGING_TABLE = "staging_track_history"

# COMMAND ----------

# Fetch the latest record from raw_track_history
latest_raw_json = spark.sql("""
    SELECT raw_data, ingestion_time 
    FROM spotify.raw_track_history 
    ORDER BY ingestion_time DESC 
    LIMIT 1
""").collect()[0]['raw_data']

# COMMAND ----------

try:
    # try to clean and transform data into valid pydantic model
    transformed_df  = clean_recently_played(latest_raw_json)
except ValidationError as e:
        print(e)



# COMMAND ----------

# validate the recently played tracks dataframe for table requirements
if is_played_data_valid(transformed_df):
    transformed_df = (spark.createDataFrame(transformed_df)
                      .withColumn("played_at", col("played_at").cast("timestamp"))
                      .withColumn("duration_ms", col("duration_ms").cast(IntegerType()))
                      .withColumn("popularity", col("popularity").cast(IntegerType()))
                      )

    # Overwrite the staging table with transformed data
    (transformed_df
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(f"spotify.{STAGING_TABLE}")
    )

