from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pyodbc
import pandas as pd
import os

def extract(db_str: str, dir):
    """Loop through all files in the directory to load what we can into tables of the same names."""
    try:
        # starting directory
        directory = dir
        # iterate over files in the directory
        for filename in os.listdir(directory):
            #get filename without ext
            file_wo_ext = os.path.splitext(filename)[0]
            # only process excel files
            if filename.endswith(".xlsx"):
                f = os.path.join(directory, filename)
                # checking if it is a file
                if os.path.isfile(f):
                    df = pd.read_excel(f)
                    # call to load
                    load(db_str, df, file_wo_ext)
            if filename.endswith(".csv"):
                f = os.path.join(directory, filename)
                # checking if it is a file
                if os.path.isfile(f):
                    df = pd.read_csv(f)
                    # call to load
                    load(db_str, df, file_wo_ext)
            if filename.endswith(".json"):
                f = os.path.join(directory, filename)
                if os.path.isfile(f):
                    df = pd.read_json(f)
                    # call to load
                    load(db_str, df, file_wo_ext)
            # I don't want to add xml or other file formats here but if I need to I will later.
            
    except Exception as e:
        print("Data extract error: " + str(e))

def load(db_str: str, df: pd.DataFrame, tbl: str):
    """ Load data into database"""
    try:
        engine = create_engine(db_str)
        #left a gotcha here with dumping this into a staging table for the transforms to happend in the db after loading.
        df.to_sql(f"stg_{tbl}", engine, if_exists='replace', index=False)

    except Exception as e:
        print("Data load error: " + str(e))
    
if(__name__ == "__main__"):
    #get password from environmnet var
    pwd = os.environ['DB_PASS']
    uid = os.environ['DB_UID']
    port = os.environ['DB_PORT']
    db = os.environ['DB_NAME']
    server = os.environ['DB_SERVER']
    servertypename = os.environ['SVR_TYPE']
    db_str = f'{servertypename}://{uid}:{pwd}@{server}:{port}/{db}'
    
    source_dir = r'\\YourPC\Shared\Data\Accounts'

    if  servertypename == "mssql":
        driver = "{SQL Server Native Client 11.0}"
        connection_string = 'DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd
        db_str = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    
    #dumb down simple singel table usage.
    try:
        #call extract function, it calls load fo each file found.
        extract(db_str, source_dir)

    except Exception as e:
        print("Error while extracting data: " + str(e))