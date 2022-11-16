import sqlalchemy as sal
from sqlalchemy import create_engine
from sqlalchemy import event
import urllib
import pyodbc
import pandas as pd
import pypyodbc
import uuid
pypyodbc.lowercase = False

conn_source = pypyodbc.connect('Driver={SQL Server};'
                      'Server=GM-HUSEYINA-NB;'
                      'Database=AdventureWorks2019;'
                      'Trusted_Connection=yes;')
conn_target = pypyodbc.connect('Driver={SQL Server};'
                      'Server=GM-HUSEYINA-NB;'
                      'Database=DWH_WITH_PYHTON;'
                      'Trusted_Connection=yes;')
mssql_engine_source = create_engine(
    "mssql+pyodbc://GM-HUSEYINA-NB/AdventureWorks2019?driver=ODBC+Driver+17+for+SQL+Server",
    # disable default reset-on-return scheme
    pool_reset_on_return=None,
)
mssql_engine_target = create_engine(
    "mssql+pyodbc://GM-HUSEYINA-NB/DWH_WITH_PYHTON?driver=ODBC+Driver+17+for+SQL+Server",
    # disable default reset-on-return scheme
    pool_reset_on_return=None,
)

df = pd.read_sql(
    'select * from [HumanResources].[Shift]'
    , con=mssql_engine_source)
print(df.info())
print(df.info())



df.to_sql("Shift", con=mssql_engine_target,
           if_exists='append', schema="dbo",
           index=False)
