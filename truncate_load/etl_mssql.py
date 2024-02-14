#import needed libraries
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd
import os


driver = "{SQL Server Native Client 11.0}"

#extract data from sql server
def extract(server: str, database: str, uid: str, pwd: str, tbl: str) -> tuple[pd.DataFrame, str]:
    try:
        connection_string = 'DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        src_engine = create_engine(connection_url)
        src_conn = src_engine.connect()
        # execute query
        return (pd.read_sql_query(f'select * FROM {tbl}', src_conn), tbl)

    except Exception as e:
        print("Data extract error: " + str(e))
        
def extract_multiple(server: str, database: str, uid: str, pwd: str, qry: str, target_server: str, target_db: str, target_uid: str, target_pwd: str):
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
            load(target_server, target_db, target_uid, target_pwd, df, table_name)

    except Exception as e:
        print("Data extract error: " + str(e))

#load data to postgres
def load(server: str, database: str, uid: str, pwd: str, df: pd.DataFrame, tbl: str):
    try:
        connection_string = 'DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url)
        # save df to postgres
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False, chunksize=100000)
    except Exception as e:
        print("Data load error: " + str(e))

    
if(__name__ == "__main__"):
    #get password from environmnet var
    pwd = os.environ['DB_PASS']
    uid = os.environ['DB_UID']
    db = os.environ['DB_NAME']
    server = os.environ['DB_SERVER']
    target_uid = uid
    target_pwd = pwd
    #sql db details
    source_server = "./"
    database = "databasename"
    table = "table"
    
    #dumb down simple singel table usage.
    try:
        #call extract function
        df, tbl = extract(source_server, database, uid, pwd, table)
        #then load
        load(server, db, uid, pwd, df, tbl)
    except Exception as e:
        print("Error while extracting data: " + str(e))
        
    #mutlitable - That should break/return nothing if you don't correct the query to find the table names.
    #try:
    #    query = """select  t.name as table_name
    #    from sys.tables t 
    #    where t.name like 'Your mom %'  """
    #    extract_multiple((source_server, database, uid, pwd, query, server, db, target_uid, target_pwd)
    #except Exception as e:
    #    print("Error while extracting data: " + str(e))