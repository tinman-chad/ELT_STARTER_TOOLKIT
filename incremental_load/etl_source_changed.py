from sqlalchemy import create_engine, MetaData, URL
import pandas as pd
import pyodbc
import os
from datetime import datetime

#global default... I know still bad but rushing it.
default_date = datetime(1901, 1, 1, 0, 0, 0, 0)

def insertetllog(logid: int, tblname: str, rowcount: int, status: str, error: str, engine: sqlalchemy.engine):
    """ Need to track when the last update happened so we only go from there forward.
        CREATE TABLE IF NOT EXISTS etl.etlextractlog
        (
            extractlogid IDENTITY(1,1) NOT NULL,
            processlogid int NOT NULL,
            tablename varchar(200) NOT NULL,
            extractrowcount int NOT NULL DEFAULT 0,
            starttime datetime NOT NULL,
            endtime datetime,
            lastextractdatetime datetime,
            success int NOT NULL DEFAULT 0,
            status char(1) NOT NULL,
            errormessage varchar(500),
            CONSTRAINT [PK_etlextractlog_pk] PRIMARY KEY CLUSTERED 
            (
                [extractlogid] ASC
            ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
        ) ON [PRIMARY]
    """
    try:
        # set record attributes
        record = {"processlogid":logid,"tablename":tblname,"extractrowcount": rowcount,"starttime":datetime.now(),
                  "endtime":datetime.now(),"lastextractdatetime":datetime.now(),"status":status,"errormessage":error[0:490]}
        #print(record)
        #create df
        inert_etl_log = pd.DataFrame(record, index=[0])
        tbl_name = "etlextractlog"
        inert_etl_log.to_sql(tbl_name, engine, if_exists='append', index=False, schema="etl")
    except Exception as e:
        print("Unable to insert record into etl logs" + print(str(e)))
        
def getLastETLRunDate(tblName: str, engine: sqlalchemy.engine) -> datetime:
    """Get the last time it ran so we can run it from there forward."""
    try:
        #
        qry_logs = pd.read_sql(f"""Select  max(lastextractdatetime) as lastETLRunDate
        from etl.etlextractlog where tablename= '{tblName}'""", engine)
        etlrundate = qry_logs['lastetlrundate'][0]
        if not etlrundate:
            etlrundate = default_date
        return etlrundate
    except Exception  as e:
        return default_date
    
def getSourceData(engine: sqlalchemy.engine, table_name: str, created: datetime, modified: datetime, deleted: datetime|None) -> pd.DataFrame:
    """Load the source data to check for differences and update the target system with."""
    deleted_where = ""
    if deleted != None:
        deleted_where = f" or DateDeleted >= {deleted}"
    source = pd.read_sql_query(f"""Select * from {table_name}
                               Where (DateCreated >= {created}
                               or DateUpdated >= {modified}
                               {deleted_where}
                               )""", engine)
    return source

def upsert(data: pd.DataFrame, table_name: str, key: str, lastrundate: datetime, engine: sqlalchemy.engine):
    """Faking an upsert here"""
    if(lastrundate== default_date):
        try: #all new just dump it in.
            data.to_sql(table_name, engine, if_exists='replace', index=False, schema="public")
            insertetllog(1, table_name, len(data), "Y", "NA")
        except Exception as e:  
            insertetllog(1, table_name, len(data), "N", str(e))
    else:
        try: # need to insert the new update the existing... should be nice and use a real upsert script here... 
            update_to_sql(data, table_name, key)
            insertetllog(1, table_name, len(data), "Y", "NA")
        except Exception as e:  
            insertetllog(1, table_name, len(data), "N", str(e))
            
def update_to_sql(data: pd.DataFrame, table_name: str, primary_key: str, engine: sqlalchemy.engine):
    update = []
    columns = []
    temp_table = f"stg_{table_name}"
    for col in data.columns:
        columns.append(col)
        if col == primary_key:
            continue
        update.append(f'"{col}"=EXCLUDED."{col}"')
    # Persist data to temp table ---- pg format currently other templates in the templates file.
    data.to_sql(temp_table, engine, if_exists='replace', index=False, schema='public')
    update_stmt_3 = ", ".join(f' "{c}" ' for c in columns )
    insert_stmt_1 = f' INSERT INTO {table_name} ( {update_stmt_3} ) '
    insert_stmt_2 = f' Select * from {temp_table} '
    insert_stmt_3 = f' ON CONFLICT ("{primary_key}") '
    insert_stmt_4 = f' DO UPDATE SET '
    update_stmt_1 = ", ".join(update)

    upsert_stmt = insert_stmt_1 + insert_stmt_2 + insert_stmt_3 + insert_stmt_4 + update_stmt_1 +  ";"
    
    with engine.begin() as cnx:
        cnx.execute(upsert_stmt)
        
        
    
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
    connection_string = 'DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    
    #keeping the etl processing log with the source data seems strange but whatever
    pg_engine: sqlalchemy.engine = None
    ms_engine: sqlalchemy.engine = None
    connected: int = 0
    try:
        pg_engine = create_engine(db_str)
        pg_engine.connect()
        connected = connected + 1
    except:
        print('Failed to connect to postgres')
        
    try:
        ms_engine = create_engine(connection_url)
        ms_engine.connect()
        connected = connected + 1
    except:
        print('Failed to connected to mssql')
        
    if connected >= 2: #never more than 2 but laziness... well unless I also need to put it some additional place
        last_run = getLastETLRunDate("table", ms_engine)
        data = getSourceData(ms_engine, "table", last_run, last_run, last_run)
        upsert(data, "table", "id", last_run, pg_engine)