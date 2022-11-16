import json
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import event
import urllib
import pyodbc
import pandas as pd
import pypyodbc
import multiprocessing
from datetime import datetime
from dateutil.relativedelta import relativedelta
import Load_Functions


###Insert Started log
def starting_load_log(step_name,target_name):
    db_name="DWH_WITH_PYHTON"
    insert_command=f'INSERT INTO etl_logs.etl_load_logs VALUES(\'{step_name}\',\'Insert\',\'{target_name}\',GETDATE(),NULL)'
    conn=Load_Functions.define_engine(db_name).connect()
    insert_log=sqlalchemy.text(insert_command)
    conn.execution_options(autocommit=True).execute(insert_log)

###For update starting_load_log function record's finished_date
###Because sometimes loading takes long time and we should update after finished our mapping
def finish_load_log(target_name):
    db_name="DWH_WITH_PYHTON"
    get_last_log_sql=f'SELECT MAX(Id) as max_id FROM etl_logs.etl_load_logs WHERE TargetTable=\'{target_name}\''
    get_data=pd.read_sql(get_last_log_sql,con=Load_Functions.define_engine(db_name))
    last_id=get_data["max_id"][0]
    update_command=f'UPDATE etl_logs.etl_load_logs SET EndTime=GETDATE() WHERE Id={last_id} AND Operation=\'Insert\''
    conn=Load_Functions.define_engine(db_name).connect()
    update_log=sqlalchemy.text(update_command)
    conn.execution_options(autocommit=True).execute(update_log)

###Truncate log
def table_truncate_log(step_name,target_name):
    db_name="DWH_WITH_PYHTON"
    insert_command=f'INSERT INTO etl_logs.etl_load_logs VALUES(\'{step_name}\',\'Truncate\',\'{target_name}\',GETDATE(),GETDATE())'
    conn=Load_Functions.define_engine(db_name).connect()
    insert_log=sqlalchemy.text(insert_command)
    conn.execution_options(autocommit=True).execute(insert_log)

##Drop Log
def table_drop_log(step_name,target_name):
    db_name="DWH_WITH_PYHTON"
    insert_command=f'INSERT INTO etl_logs.etl_load_logs VALUES(\'{step_name}\',\'Drop\',\'T_{target_name}\',GETDATE(),GETDATE())'
    conn=Load_Functions.define_engine(db_name).connect()
    insert_log=sqlalchemy.text(insert_command)
    conn.execution_options(autocommit=True).execute(insert_log)

##Create Log
def table_create_log(step_name,target_name):
    db_name="DWH_WITH_PYHTON"
    insert_command=f'INSERT INTO etl_logs.etl_load_logs VALUES(\'{step_name}\',\'Create\',\'T_{target_name}\',GETDATE(),GETDATE())'
    conn=Load_Functions.define_engine(db_name).connect()
    insert_log=sqlalchemy.text(insert_command)
    conn.execution_options(autocommit=True).execute(insert_log)

###Upsert Last Succeded Date for CDC
def upsert_last_succeded_date(target_table,started_date):
    db_name="DWH_WITH_PYHTON"
    df = pd.read_sql(
        f'SELECT Id FROM etl_logs.last_succeded_date WHERE TableName=\'{target_table}\''
        , con=Load_Functions.define_engine(db_name))
    if len(df["Id"])==0:
        query=f'INSERT INTO etl_logs.last_succeded_date VALUES (\'{target_table}\',CONVERT(DATETIME2(0),\'{started_date}\') )'
    else:
        query=f'UPDATE etl_logs.last_succeded_date SET LastSucceededDate=CONVERT(DATETIME2(0),\'{started_date}\')  WHERE Id={df["Id"][0]}'
    conn=Load_Functions.define_engine(db_name).connect()
    log_query=sqlalchemy.text(query)
    conn.execution_options(autocommit=True).execute(log_query)

###Get Last Succeded Date for Cdc method
def get_last_succeded_date(target_table):
    db_name="DWH_WITH_PYHTON"
    df = pd.read_sql(
        f'SELECT CONVERT(DATETIME2(0),LastSucceededDate) LastSucceededDate FROM etl_logs.last_succeded_date WHERE TableName=\'{target_table}\''
        , con=Load_Functions.define_engine(db_name))
    if len(df["LastSucceededDate"])==0:
        LastSucceededDate='1900-01-01'
    else:
        LastSucceededDate=df["LastSucceededDate"][0]
    return LastSucceededDate

###delete log for cdc
def delete_target_log(step_name,target_table):
    db_name = "DWH_WITH_PYHTON"
    insert_command = f'INSERT INTO etl_logs.etl_load_logs VALUES(\'{step_name}\',\'Delete\',\'{target_table}\',GETDATE(),GETDATE())'
    conn = Load_Functions.define_engine(db_name).connect()
    insert_log = sqlalchemy.text(insert_command)
    conn.execution_options(autocommit=True).execute(insert_log)


