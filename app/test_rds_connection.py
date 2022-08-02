import psycopg2
import os
import boto3


DB_USER = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
PORT="5432"
REGION = "eu-central-1"


session = boto3.Session(profile_name='default')
client = session.client('rds')

token = client.generate_db_auth_token(DBHostname=DB_HOST, Port=PORT, DBUsername=DB_USER, Region=REGION)
try:
    connection = psycopg2.connect(f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={PORT}")

    cursor=connection.cursor()

    print(connection)
    print(cursor)
    cursor.execute("""SELECT now()""")
    query_results = cursor.fetchall()
    print(query_results)
    print("Database connection established.")
except Exception as e:
    print(f"Database connection failed due to {e}")   