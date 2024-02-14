from sqlalchemy import create_engine, MetaData, URL
import pandas as pd
import pyodbc
import os
from datetime import datetime


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

    
def getData(engine: sqlalchemy.engine, table_name: str) -> pd.DataFrame:
    """Load the data to check for differences and update the target system with."""

    df = pd.read_sql_query(f"""Select * from {table_name}""", engine)
    return df

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
        msdata = getData(ms_engine, "table")
        pgdata = getData(pg_engine, "table")
        
        changeddata = msdata[~msdata.apply(tuple, 1).isin(pgdata.apply(tuple, 1))]
        
        newdata = msdata[~changeddata.pk_id.isin(pgdata.pk_id)]
        
        update_to_sql(changeddata, "table", "pk_id", pg_engine) #really I already know they exist in the table could just to an update but...
        
        newdata.to_sql("table", pg_engine, if_exists="append", index=False)
        
        