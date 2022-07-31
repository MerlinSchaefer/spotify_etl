import psycopg2
import os

db_user = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")



connection = psycopg2.connect(f"dbname={db_name} user={db_user} password={db_password} host={db_host} port=5432")

cursor=connection.cursor()

print(connection)
print(cursor)