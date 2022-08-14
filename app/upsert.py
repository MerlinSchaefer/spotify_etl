import pandas as pd
import psycopg2
from typing import Any

# create type as mypy struggles with psycopg2 Connection
DBConnection = Any


def upsert_df(
    df: pd.DataFrame, table_name: str, primary_key: str, connection: DBConnection
) -> bool:
    """Implements the equivalent of pd.DataFrame.to_sql(..., if_exists='update')
    (which does not exist). Creates or updates the db records based on the
    dataframe records.
    Conflicts to determine update are based on the specified primary key.
    1. Check if table exists
    2. Insert/update from temp table into table_name
    3. Does NOT commit the changes to the database.
       This should be done seperately after a successful upsert.
    Parameters
    ----------
    df : pd.DataFrame
        The dataframe of to upsert into the table.
    table_name : str
        The name of the table to upsert into.
    primary_key : str
        The name of the primary key column.
    connection : psycopg2.Connection
        The connection to the database.

    Returns
    -------
    bool
        True if successful, False otherwise.
    """
    table_exists = check_table_exists(table_name, connection)
    if not table_exists:
        return False

    # If it already exists
    cursor = connection.cursor()
    # Insert/update into table_name
    headers = list(df.columns)
    headers_sql_txt = ", ".join(
        [f'"{i}"' for i in headers]
    )  # index1, index2, ..., column 1, col2, ...

    # col1 = exluded.col1, col2=excluded.col2
    update_column_stmt = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in headers])

    # Compose and execute upsert query
    query_upsert = f"""
    INSERT INTO "{table_name}" ({headers_sql_txt}) 
    VALUES {','.join(str(value) for value in list(df.to_records(index=False)))}
    ON CONFLICT ({primary_key}) DO UPDATE 
    SET {update_column_stmt};
    """
    cursor.execute(query_upsert)

    return True


def check_table_exists(table_name: str, connection: DBConnection) -> Any:
    """Checks if a table exists in the database.
    Parameters
    ----------
    table_name : str
        The name of the table to check.
    connection : psycopg2.Connection
        The connection to the database.
    Returns
    -------
    bool
        True if the table exists, False otherwise.
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
    return query_results[0][0]
