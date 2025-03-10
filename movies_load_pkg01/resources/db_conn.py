from sqlalchemy import create_engine
import pyodbc

def db_conn():
    con_str = "mssql+pyodbc://(localdb)\\mssqllocaldb/ONLAB?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(con_str)
    return engine