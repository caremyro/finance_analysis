import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():    
    connection = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    return connection

def get_db_cursor(connection):
    cursor = connection.cursor(dictionary=True)
    return cursor

def close_db_connection(connection):
    if connection.is_connected():
        connection.close()  

def cursor_close(cursor):
    if cursor:
        cursor.close()
