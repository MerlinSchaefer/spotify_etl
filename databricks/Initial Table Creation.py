# Databricks notebook source
import os

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS spotify;")

# COMMAND ----------

# List all SQL files in the directory
sql_files = [file for file in os.listdir("./table_definitions") if file.endswith('.sql')]

# Read and execute each SQL file
for file in sql_files:
    print(f"Creating table: {file}")
    with open(f"./table_definitions/{file}", 'r') as sql_file:
        sql_command = sql_file.read()
        spark.sql(sql_command)

# COMMAND ----------

# Load audio_features.csv data dump
audio_features_df = spark.read.csv("dbfs:/FileStore/data_dump/audio_features_dump.csv", header=True, inferSchema=True)
# Load track_history.csv data dump
audio_features_metadata_df = spark.read.csv("dbfs:/FileStore/data_dump/audio_features_metadata_dump.csv", header=True, inferSchema=True)
# Load audio_features_metadata.csv data dump
track_history_df = spark.read.csv("dbfs:/FileStore/data_dump/track_history_dump.csv", header=True, inferSchema=True)



# COMMAND ----------

# Show the first few rows to verify the data
audio_features_df.display()


# COMMAND ----------

audio_features_metadata_df.display()

# COMMAND ----------

track_history_df.display()

# COMMAND ----------

audio_features_metadata_df.drop("_c0").drop("id").write.format("delta").mode("overwrite").saveAsTable("spotify.audio_features_metadata")

# COMMAND ----------

# track_history_df.drop("_c0").write.format("delta").mode("append").saveAsTable("spotify.track_history")
# audio_features_df.drop("_c0").write.format("delta").mode("append").saveAsTable("spotify.audio_features")
