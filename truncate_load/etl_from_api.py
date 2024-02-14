#"""
#Get data from a url and push that to a database
#"""
import base64
import requests
import pandas as pd
from sqlalchemy import create_engine

def extract_get_only(url: str)-> dict:
    """
    Pull API unauthenticated data
    """
    
    data = requests.get(url).json()
    return data

def extract_get_basic(url: str, user: str, password: str)-> dict:
    """
    Pull API basic authenticated data
    """
    authval = base64.b64encode(f"{user}:{password}")
    data = requests.get(url, headers={"Authorization": f"Basic {authval}"}).json()
    return data

def extract_get_bearer(url: str, token: str) -> dict:
    """
    Pull API bearer (jwt) authenticated data
    """
    data = requests.get(url, headers={"Authorization": f"Bearer {token}"}).json()
    return data

def transform(data:dict) -> pd.DataFrame:
    """ Transforms the dataset into desired structure and filters"""
    df = pd.DataFrame(data)
    #Do any transforms/clean up/yada yada here
    return df

def load(db_str: str, tbl_name: str, df:pd.DataFrame)-> None:
    """ Loads data into a sqllite database"""
    db_engine = create_engine(db_str)
    df.to_sql(tbl_name, db_engine, if_exists='replace')


if(__name__ == "__main__"):
    #dummy variables
    database = "sqlite:///data_dumpster.db"
    table = "table_name"
    source_url = "example.com/api/get_data"
    #simplton setup.
    data = extract_get_only(source_url)
    df = transform(data)
    load(database, table, df)