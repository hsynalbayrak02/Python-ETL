import json
import time
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
import etl_logs
import create_table_functions

###Get columns Name For Select
def get_columns_name(table_name):
    column_list_txt=""
    comma=""
    columns_json=open("Table_Columns_Infos/"+table_name+".json","r")
    json_data=json.load(columns_json)
    for i in range(0,len(json_data["table_columns"])):
        if i==len(json_data["table_columns"])-1:
            comma=""
        else:
            comma=","
        # column_list.append(dict(json_data["table_columns"][i])["name"])
        # column_type.append(dict(json_data["table_columns"][i])["type"])
        column_list_txt=column_list_txt+dict(json_data["table_columns"][i])["name"]+comma
    return column_list_txt

###Define Engine for Chosen DB
def define_engine(db_name):
    all_db_names=[]
    db_index=None
    db_infos_json=open("General_Parameters/DB_Infos.json","r")
    json_data = json.load(db_infos_json)
    for i in range(0,len(json_data["dbs"])):
        all_db_names.append(dict(json_data["dbs"][i])["db_name"])
    for j in range(0,len(all_db_names)):
        if all_db_names[j]==db_name:
            db_index=j
    if db_index==None:
        raise ValueError('Your DB cannot found in DB list,check again!!')
    db_type=dict(json_data["dbs"][db_index])["db_type"]
    db_name=dict(json_data["dbs"][db_index])["db_name"]
    server_name=dict(json_data["dbs"][db_index])["server_name"]
    driver_type=dict(json_data["dbs"][db_index])["driver"]
    engine = create_engine(
        f'{db_type}+pyodbc://{server_name}/{db_name}?driver={driver_type}'
    )
    return engine

###Get Data From Source Table To Insert Target Table
def get_data(table_name,target_table=None):
    db_name,schema_name,from_name=table_name.split("-")
    column_list=get_columns_name(table_name)
    cdc_column = get_cdc_column(table_name)
    last_succeeded_date=etl_logs.get_last_succeded_date(target_table)
    if cdc_column=="":
        select_query=f'select {column_list} from {schema_name}.{from_name}'
    else:
        select_query= f'select {column_list} from {schema_name}.{from_name} WHERE {cdc_column}>=\'{last_succeeded_date}\''
    df = pd.read_sql(
        select_query
        , con=define_engine(db_name))
    return df

###Insert Target Table
def insert_ods_table(source_table,target_table):
    started_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    db_name,schema_name,insert_name=target_table.split("-")
    column_list=get_columns_name(target_table)
    etl_logs.starting_load_log("ODS",insert_name)
    get_data(source_table).to_sql(insert_name, con=define_engine(db_name),
              if_exists='append', schema=schema_name,
              index=False)
    etl_logs.finish_load_log(insert_name)
    etl_logs.upsert_last_succeded_date(target_table,started_date)

###insert temp table for cdc method
def insert_temp_table(source_table,target_table):
    global started_date_cdc
    db_name,schema_name,insert_name=target_table.split("-")
    started_date_cdc=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    column_list=get_columns_name(target_table)
    insert_name="T_"+insert_name
    etl_logs.starting_load_log("ODS",insert_name)
    get_data(source_table,target_table).to_sql(insert_name, con=define_engine(db_name),
              if_exists='append', schema=schema_name,
              index=False)
    etl_logs.finish_load_log(insert_name)

###Truncate Target Table
def truncate_target_table(table_name):
    db_name, schema_name, truncate_name = table_name.split("-")
    conn=define_engine(db_name).connect()
    truncate_query=sqlalchemy.text(f'TRUNCATE TABLE {schema_name}.{truncate_name}')
    conn.execution_options(autocommit=True).execute(truncate_query)
    etl_logs.table_truncate_log("ODS",truncate_name)

##Drop temp table for cdc method
def drop_table(target_table):
    db_name, schema_name, table_name = target_table.split("-")
    conn = define_engine(db_name).connect()
    drop_query = sqlalchemy.text(f'DROP TABLE IF EXISTS {schema_name}.T_{table_name}')
    conn.execution_options(autocommit=True).execute(drop_query)
    etl_logs.table_drop_log("ODS",table_name)


###create temp table for cdc method
def create_temp_table(target_table):
    db_name, schema_name, table_name = target_table.split("-")
    drop_table(target_table)
    conn = define_engine(db_name).connect()
    columns_text=""
    comma=","
    column_count=create_table_functions.get_column_count(target_table)
    for i in range(0,column_count):
        name,type,mand=create_table_functions.get_column_infos(i,target_table)
        if i!=column_count-1:
            columns_text+=f'{name} {type} {mand} {comma}\n'
        else:
            columns_text += f'{name} {type} {mand}'
    create_query = sqlalchemy.text(f'CREATE TABLE {schema_name}.T_{table_name} ({columns_text})')
    conn.execution_options(autocommit=True).execute(create_query)
    etl_logs.table_create_log("ODS", table_name)

##get pk column for check existing rows in target
def get_pk_column(target_table):
    columns_json=open("Table_Columns_Infos/"+target_table+".json","r")
    json_data=json.load(columns_json)
    pk_column=json_data["pk_column"]
    return pk_column

##get column for create temp table
def get_cdc_column(source_table):
    columns_json=open("Table_Columns_Infos/"+source_table+".json","r")
    json_data=json.load(columns_json)
    cdc_column=json_data["cdc_columns"]
    return cdc_column

###delete existing rows for cdc method
def delete_existing_rows(target_table):
    db_name, schema_name, table_name = target_table.split("-")
    pk_column=get_pk_column(target_table)
    conn = define_engine(db_name).connect()
    delete_query = sqlalchemy.text(f'DELETE FROM {schema_name}.{table_name}  WHERE EXISTS (SELECT 1 FROM {schema_name}.T_{table_name} b WHERE {schema_name}.{table_name}.{pk_column}=b.{pk_column})')
    conn.execution_options(autocommit=True).execute(delete_query)
    etl_logs.delete_target_log("ODS", table_name)

###insert temp table for cdc method
def insert_target_from_temp(target_table):
    global started_date_cdc
    db_name, schema_name, table_name = target_table.split("-")
    etl_logs.starting_load_log("ODS", table_name)
    conn = define_engine(db_name).connect()
    delete_query = sqlalchemy.text(f'INSERT INTO {schema_name}.{table_name} SELECT * FROM {schema_name}.T_{table_name} ')
    conn.execution_options(autocommit=True).execute(delete_query)
    etl_logs.upsert_last_succeded_date(target_table, started_date_cdc)
    etl_logs.finish_load_log(table_name)

##Finalize truncate insert method
def load_data_method_truncate_insert(source_table,target_table):
    truncate_target_table(target_table)
    insert_ods_table(source_table,target_table)

##Finalize CDC method
def load_data_cdc(source_table,target_table):
    create_temp_table(target_table)
    insert_temp_table(source_table, target_table)
    delete_existing_rows(target_table)
    insert_target_from_temp(target_table)
    drop_table(target_table)

###This function makes multi progress your whole mappings in ODS
def ods_paralell_run():
    if __name__ == '__main__':
        ods_tables = open("Tables_And_Load_Strategy/ODS_Tables.json", "r")
        json_data = json.load(ods_tables)
        mappings=[]
        for data in json_data["tables"]:
            type=dict(data)["load_type"]
            source_name=dict(data)["source_name"]
            target_name=dict(data)["target_name"]
            if get_mapping_run_status(dict(data)["source_name"]) & (dict(data)["status"]=="ok"):
                if type=="load_data_method_truncate_insert":
                    mapping=multiprocessing.Process(target=load_data_method_truncate_insert(source_name,target_name), args=[2])
                    mapping.start()
                    mappings.append(mapping)
                elif type=="load_data_cdc":
                    mapping=multiprocessing.Process(target=load_data_cdc(source_name,target_name), args=[2])
                    mapping.start()
                    mappings.append(mapping)
            for mapping in mappings:
                mapping.join()

###This function control schedule type of mapping
##If running time not correct for schedule type then it will not run
def get_mapping_run_status(source_table):
    ods_tables = open("Tables_And_Load_Strategy/ODS_Tables.json", "r")
    json_data = json.load(ods_tables)
    index=None
    for i in range(0,len(json_data["tables"])):
       if dict(json_data["tables"][i])["source_name"]==source_table:
            index=i
    if index == None:
        raise ValueError('Your source table cannot found in table list,check again!!')
    if dict(json_data["tables"][index])["schedule_type"]==1:
        return True
    elif dict(json_data["tables"][index])["schedule_type"]==2:
        if datetime.now().strftime("%d")=="01":
            return True
    elif dict(json_data["tables"][index])["schedule_type"]==3:
        if datetime.now().strftime("%d") == "02":
            return True
    elif dict(json_data["tables"][index])["schedule_type"] == 4:
        if datetime.now().day == ((datetime.now()+relativedelta(day=31)).date()).day:
         return True


###Control running status for DM mappings
def get_mapping_run_status_dm(target_table):
    ods_tables = open("Tables_And_Load_Strategy/DM_Tables.json", "r")
    json_data = json.load(ods_tables)
    index=None
    for i in range(0,len(json_data["tables"])):
       if dict(json_data["tables"][i])["target_name"]==target_table:
            index=i
    if index == None:
        raise ValueError('Your source table cannot found in table list,check again!!')
    if dict(json_data["tables"][index])["schedule_type"]==1:
        return True
    elif dict(json_data["tables"][index])["schedule_type"]==2:
        if datetime.now().strftime("%d")=="01":
            return True
    elif dict(json_data["tables"][index])["schedule_type"]==3:
        if datetime.now().strftime("%d") == "02":
            return True
    elif dict(json_data["tables"][index])["schedule_type"] == 4:
        if datetime.now().day == ((datetime.now()+relativedelta(day=31)).date()).day:
         return True

###Run all your mappings serial
def dm_serial_run():
    if __name__ == '__main__':
        ods_tables = open("Tables_And_Load_Strategy/DM_Tables.json", "r")
        json_data = json.load(ods_tables)
        for data in json_data["tables"]:
            type=dict(data)["load_type"]
            target_name=dict(data)["target_name"]
            if get_mapping_run_status_dm(dict(data)["target_name"]) & (dict(data)["status"]=="ok"):
                if type=="truncate_insert_dm_method":
                    truncate_insert_dm_method(target_name)
                elif type=="load_data_dm":
                    load_data_dm(target_name)

###Load data to DM for append or truncate insert method
def load_data_dm(target_table):
    sql_con=open("Table_Columns_Infos/DM/"+target_table+".sql","r")
    sql_file=sql_con.read()
    sql_con.close()
    db_name,schema_name,table_name=target_table.split("-")
    etl_logs.starting_load_log("DM",table_name)
    conn=define_engine(db_name).connect()
    run_query=sqlalchemy.text(sql_file)
    conn.execution_options(autocommit=True).execute(run_query)
    etl_logs.finish_load_log(table_name)

###Truncate insert method for DM layer
def truncate_insert_dm_method(target_table):
    truncate_target_table(target_table)
    load_data_dm(target_table)


ods_paralell_run()
dm_serial_run()




###hata alırsa bir daha dene ods için

###dm katmanı sql çalıştıracak

###used by parser yazılacak ilgili tablolar

# if __name__ == '__main__':
    # print(get_mapping_run_status_dm("DWH_WITH_PYHTON-dm-currency_sumary"))
    # truncate_insert_dm_method("DWH_WITH_PYHTON-dm-currency_sumary")
    # print(get_data("AdventureWorks2019-Person-Person","DWH_WITH_PYHTON-dbo-Person" ))
    # load_data_cdc("AdventureWorks2019-Person-Person","DWH_WITH_PYHTON-dbo-Person")
    # insert_temp_table("AdventureWorks2019-Person-Person","DWH_WITH_PYHTON-dbo-Person")
    # insert_ods_table("AdventureWorks2019-Person-Person","DWH_WITH_PYHTON-dbo-Person")
    # print(delete_existing_rows("DWH_WITH_PYHTON-dbo-Person"))

