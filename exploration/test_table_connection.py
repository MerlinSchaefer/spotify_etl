import psycopg2
import os
import pandas as pd

db_user = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")


connection = psycopg2.connect(
    f"dbname={db_name} user={db_user} password={db_password} host={db_host} port=5432"
)

cursor = connection.cursor()



track_history_query = """
SELECT * 
FROM track_history
ORDER BY played_at;
"""

track_history_count_query = """
SELECT COUNT(*) AS num_records 
FROM track_history;
"""

audio_features_query = """
SELECT * 
FROM audio_features
LIMIT 10;
"""
audio_features_count_query = """
SELECT COUNT(*) AS num_records 
FROM audio_features;
"""

audio_features_metadata_query = """
SELECT * 
FROM audio_features_metadata
"""

print("Testing track_history")
print(pd.read_sql(track_history_query, con=connection))
print(pd.read_sql(track_history_count_query, con=connection))
print("Testing audio_features_query")
print(pd.read_sql(audio_features_query, con=connection))
print(pd.read_sql(audio_features_count_query, con=connection))
print("Testing audio_features_metadata_query")
print(pd.read_sql(audio_features_metadata_query, con=connection))

