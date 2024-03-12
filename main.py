from fastapi import FastAPI
import pandas as pd
import database
import json
import datetime
#from pandas_profiling import ProfileReport
from fastapi.responses import HTMLResponse,JSONResponse
from json2html import *

app = FastAPI()

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, pd.Timestamp)):
            return obj.isoformat()
        return super().default(obj)

def join_excel_and_sql_data(excel_file_path, sql_table_name):
    # Read the Excel file
    df_excel = pd.read_excel(excel_file_path)

    # Read the SQL data
    df_sql = database.read_sql_data_as_df(table_name=sql_table_name)

    # Join the two DataFrames
    df_joined = pd.merge(df_sql, df_excel, left_on='NAICCode', right_on='NAICS12', how='inner')

    return df_joined

################################
##['id', 'Gender', 'Age', 'Driving_License', 'Region_Code',
# 'Previously_Insured', 'Vehicle_Age', 
# 'Vehicle_Damage', 'Annual_Premium', 
# 'Policy_Sales_Channel', 'Vintage', 'Country',
# 'Region', 'Area', 'NAICCode', 'NAICTitle', 'Response', 
# 'Created_datetime', 'Created_by', 'FileName', 'BatchID', 'NAICS12', 'INDEX ITEM DESCRIPTION']
################################
@app.get("/data", response_class=HTMLResponse)
def get_joined_data():
    
    df_joined = join_excel_and_sql_data('2012_NAICS_Index_File.xls', 'crosssell_utk')
    # Convert column names to a list and print
    column_names = df_joined.columns.tolist()
    print(column_names)
      # Generate a report
    report = df_joined.describe(include='all').to_dict()

  # Convert the report to a JSON string using the custom encoder
    json_str = json.dumps(report, cls=CustomJSONEncoder)
    print(json_str)
    # Parse the JSON string to a Python object
    #data = json.loads(json_str)

    # Return the data as a JSON response
    #return json_str
    #return df_joined.to_json()
        # Convert the JSON string to an HTML document
    html_str = json2html.convert(json = json_str)
    return HTMLResponse(content=html_str)


