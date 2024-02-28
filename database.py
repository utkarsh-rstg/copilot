import pyodbc
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define your connection string
conn_str = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=copilotmaster.database.windows.net;'
    r'DATABASE=copilotmaster;'
    r'UID=copilot_suser;'
    r'PWD=Admin@123;'
)

def list_all_tables():
    try:
        # Create a new connection
        logging.info('Connecting to the database...')
        conn = pyodbc.connect(conn_str)

        # Create a cursor from the connection
        cursor = conn.cursor()

        # Get a list of all tables
        logging.info('Fetching all tables...')
        tables = cursor.execute("""
            SELECT table_name 
            FROM INFORMATION_SCHEMA.TABLES 
        """).fetchall()
        logging.info(tables)
        # For each table, execute a SELECT * query and print the results
        for table in tables:
            table_name = table[0]
            print(f"Table: {table_name}")

    except Exception as e:
        logging.error(f'An error occurred: {e}')

    finally:
        # Close the connection
        logging.info('Closing the database connection...')
        conn.close()

def list_all_columns_of_table(table_name:str):
    # Create a new connection
    conn = pyodbc.connect(conn_str)

    # Create a cursor from the connection
    cursor = conn.cursor()


    # Get the column names of the table
    columns = cursor.execute(f"""
        SELECT column_name
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{table_name}' and table_catalog = 'copilotmaster'
    """).fetchall()

    # Print the column names
    for column in columns:
        print(column[0])

    # Close the connection
    conn.close()


list_all_tables()
list_all_columns_of_table('crosssell_utk')