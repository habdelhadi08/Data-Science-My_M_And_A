import sqlite3 as sql
import csv
import pandas as pd

def sql_to_csv(database, table_name):
    connection = sql.connect(database)
    df = pd.read_sql(f"SELECT * FROM {table_name}",connection)
    return df.to_csv(index=False).rstrip('\n')

def csv_to_sql(csv_content, database, table_name):
    connection = sql.connect(database)
    df = pd.read_csv(csv_content)
    return df.to_sql(name=table_name, con=connection, index=False)