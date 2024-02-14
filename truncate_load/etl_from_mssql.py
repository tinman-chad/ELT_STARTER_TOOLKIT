#import needed libraries
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd
import os

#extract data from sql server
def extract(driver: str, server: str, database: str, uid: str, pwd: str, tbl: str) -> tuple[pd.DataFrame, str]:
    try:
        connection_string = 'DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        src_engine = create_engine(connection_url)
        src_conn = src_engine.connect()
        # execute query
        return (pd.read_sql_query(f'select * FROM {tbl}', src_conn), tbl)

    except Exception as e:
        print("Data extract error: " + str(e))
        
def extract_multiple(driver: str, server: str, database: str, uid: str, pwd: str, qry: str, target_db: str):
    try:
        connection_string = 'DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        src_engine = create_engine(connection_url)
        src_conn = src_engine.connect()
        # execute query
        src_tables = pd.read_sql_query(qry, src_conn).to_dict()['table_name']

        for id in src_tables:
            table_name = src_tables[id]
            df = pd.read_sql_query(f'select * FROM {table_name}', src_conn)
            load(target_db, df, table_name)

    except Exception as e:
        print("Data extract error: " + str(e))

#load data to postgres
def load(db: str, df: pd.DataFrame, tbl: str):
    try:
        engine = create_engine(db)
        # save df to postgres
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False, chunksize=100000)
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
    #sql db details
    driver = "{SQL Server Native Client 11.0}"
    source_server = "./"
    database = "databasename"
    table = "table"
    #dumb down simple singel table usage.
    try:
        #call extract function
        df, tbl = extract(driver, source_server, database, uid, pwd, table)
        #then load
        
        load(db_str, df, tbl)
    except Exception as e:
        print("Error while extracting data: " + str(e))
        
    #mutlitable - That should break/return nothing if you don't correct the query to find the table names.
    #try:
    #    query = """select  t.name as table_name
    #    from sys.tables t 
    #    where t.name like 'Your mom %'  """
    #    extract_multiple((driver, source_server, database, uid, pwd, query, db_str)
    #except Exception as e:
    #    print("Error while extracting data: " + str(e))