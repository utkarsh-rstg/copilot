import pyodbc
import pandas as pd
import logging
from datetime import datetime

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
    
def list_records_of_table(table_name:str):
    # Create a new connection
    conn = pyodbc.connect(conn_str)

    # Create a cursor from the connection
    cursor = conn.cursor()


    # Get the column names of the table
    records = cursor.execute(f"""
        SELECT top 10 * FROM {table_name}
    """).fetchall()

    for record in records:
        print(record)

    # Close the connection
    conn.close()
    
def convert_path(path):
    return path.replace("\\", "/")

def read_csv_and_load():
    try:
        # Read the CSV file
        unix_path = convert_path('C:\\Users\\admin\\Desktop\\utkarsh_test_files\\download\\data1.csv')
        print(unix_path)
        data = pd.read_csv(unix_path)

        # Add the missing columns
        data['Created_datetime'] = datetime.now()  # current date and time
        data['FileName'] = 'data1.csv'  # the name of the CSV file
        # Generate a default batch ID using the current timestamp
        data['BatchID'] = 1
        print(data.columns)
        data = data.drop('Country', axis=1)
        top_1000 = data.head(1000)
        # Establish a connection to your SQL Server
        conn = pyodbc.connect(conn_str)

        cursor = conn.cursor()

        # Prepare the insert statement
        insert_stmt = """
        INSERT INTO crosssell_utk (id, Gender, Age, Driving_License, Region_Code, Previously_Insured, Vehicle_Age, Vehicle_Damage, Annual_Premium, Policy_Sales_Channel, Vintage, Region, Area, NAICTitle, Response, Created_datetime, FileName, BatchID)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Perform the bulk insert
        cursor.executemany(insert_stmt, top_1000.values.tolist())

        conn.commit()
        # Close the connection
        conn.close()
    except pd.errors.EmptyDataError:
        print("No data found in the CSV file.")
    except pyodbc.Error as ex:
        sqlstate = ex.args[1] if len(ex.args) > 1 else None
        print(f"An error occurred: {str(ex)}, SQLSTATE: {sqlstate}")
    except Exception as ex:
        print(f"An unexpected error occurred: {str(ex)}")
    

#list_all_tables()
list_all_columns_of_table('crosssell_utk')
#read_csv_and_load()
list_records_of_table('crosssell_utk')