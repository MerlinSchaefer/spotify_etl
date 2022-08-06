import pandas as pd
import psycopg2
import uuid
import os
import boto3


def upsert_df(df: pd.DataFrame, table_name: str, primary_key: str, connection:psycopg2.Connection) -> bool:
    """Implements the equivalent of pd.DataFrame.to_sql(..., if_exists='update')
    (which does not exist). Creates or updates the db records based on the
    dataframe records.
    Conflicts to determine update are based on the dataframes index.
    This will set unique keys constraint on the table equal to the index names
    1. Create a temp table from the dataframe
    2. Insert/update from temp table into table_name
    Returns: True if successful
    """
    cursor = connection.cursor()
    # If the table does not exist, exit early
    cursor.execute(
        f"""SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = 'public'
            AND    table_name   = '{table_name}');
            """
    )
    query_results = cursor.fetchall()
    table_exists = query_results[0][0]
    if not table_exists:
        return False

    # If it already exists

    #  Insert/update into table_name
    headers = list(df.columns)
    headers_sql_txt = ", ".join(
        [f'"{i}"' for i in headers]
    )  # index1, index2, ..., column 1, col2, ...

    # col1 = exluded.col1, col2=excluded.col2
    update_column_stmt = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in headers])

    # # For the ON CONFLICT clause, postgres requires that the columns have unique constraint
    # query_pk = f"""
    # ALTER TABLE "{temp_table_name}" ADD CONSTRAINT unique_constraint_for_upsert UNIQUE ({primary_key});
    # """
    # cursor.execute(query_pk)
    print(headers_sql_txt)
    # Compose and execute upsert query
    query_upsert = f"""
    INSERT INTO "{table_name}" ({headers_sql_txt}) 
    VALUES {','.join(str(value) for value in list(df.to_records(index=False)))}
    ON CONFLICT ({primary_key}) DO UPDATE 
    SET {update_column_stmt};
    """
    cursor.execute(query_upsert)
    # cursor.execute(f"DROP TABLE {temp_table_name}")

    return True


DB_USER = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
PORT = "5432"
REGION = "eu-central-1"


boto3_session = boto3.Session(profile_name="default")
boto3_client = boto3_session.client("rds")

token = boto3_client.generate_db_auth_token(
    DBHostname=DB_HOST, Port=PORT, DBUsername=DB_USER, Region=REGION
)

try:
    connection = psycopg2.connect(
        f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={PORT}"
    )

    cursor = connection.cursor()

    print(connection)
    print(cursor)
    query_results = cursor.execute("""SELECT now()""")
    query_results = cursor.fetchall()
    print(query_results)
    print("Database connection established.")
except Exception as e:
    print(f"Database connection failed due to {e}")

df_tracks = pd.read_csv("played_tracks.csv", sep=";")
df_audio = pd.read_csv("audio_features.csv", sep=";")
print(df_tracks.head())
if upsert_df(df_audio, "audio_features", "id", connection) and upsert_df(
    df_tracks, "track_history", "played_at", connection
):
    print("Success")
    connection.commit()
else:
    print("Failed")
connection.close()
