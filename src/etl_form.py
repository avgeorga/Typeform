import pandas as pd
import json, os, requests, sqlite3
import awswrangler as wr
from sqlite3 import Error
from datetime import date

today = date.today()
cwd = os.getcwd()
folder = cwd + "\\src\\src_data\\"

#  create a database connection to a SQLite database
# :param db_file: database file
# :return: Connection object or None
def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn


# Extract layer - Read dataset file and return it  as data frame
#    :param source_name API address or File name
#    :retur Pandas DataFrame
def extract_data(source_name:str):
    ### In the case we execute the API, the next two line must be uncommnet ###
    #response_API = requests.get(source_name)
    #source_name = response_API.text
    ### Till here ###

    jsonF = json.load(open(source_name))
    data = pd.DataFrame(jsonF["fields"])

    #Add common parameter to the DataFrame
    data['form_id']=jsonF['id']

    #Add setting values to the DataFrame
    data = data.assign(**jsonF["settings"])    

    return data

# Tranform layer - transform data
#    :param df: Pandas dataframe contains file to transform and load it
#    :return Pandas DataFrame
def tranform_data(df):
    #Selection of the necessary columns to be loaded into the file and table.
    return df[['form_id','language','progress_bar','show_time_to_complete','show_number_of_submissions','redirect_after_submit_url','id','title','type','reference']]
    
# Tranform layer - to customer table
#    :param df: Pandas dataframe contains data to transform and load it
#    :param file_name: File name to create the output file.
#    :param conn: Connection object
def load_data(df, file_name, conn, out_format):
    if (out_format=='txt'):   
        df.to_csv(file_name, index=False)
        df.to_sql('form', conn, if_exists='replace', index=False)
    else:
        wr.s3.to_parquet(
            dataframe=df,
            path=file_name   
        )

def main():
    src_name = folder + 'form.json'
    tgt_name = folder + 'out_form.txt'
    database = folder + 'Typeform.db'

    conn = create_connection(database)
    df=extract_data(src_name)    
    df=tranform_data(df)
    load_data(df, tgt_name, conn, 'txt')

if __name__ == '__main__':
    main()
