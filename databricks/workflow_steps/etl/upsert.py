from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from databricks.sdk.runtime import spark

def upsert_df(df: DataFrame, table_name: str, primary_key: str):
    """
    Upserts a DataFrame into a Delta table in Databricks. Raises an error if the table does not exist.

    Parameters:
    df (DataFrame): The Spark DataFrame to upsert.
    table_name (str): The name of the Delta table.
    primary_key (str): The column used as the unique identifier for upserts.
    """
    try:
        # Check if the Delta table exists
        delta_table = DeltaTable.forName(spark, f"spotify.{table_name}")

        # Perform the merge/upsert
        delta_table.alias("tgt").merge(
            df.alias("src"),
            f"tgt.{primary_key} = src.{primary_key}"
        ).whenMatchedUpdateAll()\
         .whenNotMatchedInsertAll()\
         .execute()
    
    except AnalysisException as e:
        if "Table or view not found" in str(e):
            raise Exception(f"Table '{table_name}' does not exist.")
        else:
            raise e


