import pandas as pd
import database

def join_excel_and_sql_data(excel_file_path, sql_table_name):
    # Read the Excel file
    df_excel = pd.read_excel(excel_file_path)

    # Print the column names
    print(df_excel.columns)
    print(df_excel.head(5))

    # Read the SQL data
    df_sql = database.read_sql_data_as_df(table_name=sql_table_name)
    #print(df_sql.columns)
    #print(df_sql['Gender'].head(5))

    # Join the two DataFrames
    df_joined = pd.merge(df_sql, df_excel, left_on='NAICCode', right_on='NAICS12', how='inner')

    # Print the joined DataFrame
    print(df_joined.head(500))

    return df_joined


def summary_analysis():
    # Call the function
    joined_data = join_excel_and_sql_data('2012_NAICS_Index_File.xls', 'crosssell_utk')
    print(joined_data)

summary_analysis()
