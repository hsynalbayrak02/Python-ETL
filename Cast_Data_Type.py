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
import etl_logs
import Load_Functions
from sklearn.preprocessing import LabelEncoder


def get_columns_type(target_table):
    column_type=[]
    columns_json=open("Table_Columns_Infos/"+target_table+".json","r")
    json_data=json.load(columns_json)
    for i in range(0,len(json_data["table_columns"])):
        column_type.append(dict(json_data["table_columns"][i])["type"])
    return column_type

def get_columns_name(target_table):
    column_type=[]
    columns_json=open("Table_Columns_Infos/"+target_table+".json","r")
    json_data=json.load(columns_json)
    for i in range(0,len(json_data["table_columns"])):
        column_type.append(dict(json_data["table_columns"][i])["name"])
    return column_type

def get_db_type(target_table):
    all_db_names=[]
    db_index=None
    db_name=db_name=target_table.split("-")[0]
    db_infos_json=open("General_Parameters/DB_Infos.json","r")
    json_data_db_type = json.load(db_infos_json)
    for i in range(0,len(json_data_db_type["dbs"])):
        all_db_names.append(dict(json_data_db_type["dbs"][i])["db_name"])
    for j in range(0,len(all_db_names)):
        if all_db_names[j]==db_name:
            db_index=j
    if db_index==None:
        raise ValueError('Your DB cannot found in DB list,check again!!')
    db_type=dict(json_data_db_type["dbs"][db_index])["db_type"]
    return db_type

def cast_data_types(source_table,target_table):
    df=Load_Functions.get_data(source_table)
    types=get_columns_type(target_table)
    columns_json=open("General_Parameters/DB_Types_Vs_Pyhton_Types.json","r")
    json_data=json.load(columns_json)
    db_type=get_db_type(target_table)
    # columns=get_columns_name(target_table)
    if db_type == "mssql":
        for i in range(0,len(types)):
            if types[i] == "uniqueidentifier":
                encoder = LabelEncoder().fit(df.iloc[:, i])
                df.iloc[:, i] = encoder.transform(df.iloc[:, i])

            # if types[i]=="bigint" or types[i]=="int" or types[i]=="smallint" or types[i]=="tinyint" or \
            #     types[i] == "bit" or types[i]=="decimal" or types[i]=="numeric" or types[i]=="money" or types[i]=="smallmoney" or \
            #     types[i] == "float" or types[i] == "real":
            #     df.iloc[:,i]=pd.to_numeric(df.iloc[:,i])
            # elif types[i]=="datetime" or types[i]=="datetime2":
            #     df.iloc[:, i] = pd.to_datetime(df.iloc[:, i])
            # elif types[i]=="smalldatetime" or types[i]=="date":
            #     df.iloc[:, i] = pd.to_datetime(df.iloc[:, i]).dt.date
            # elif types[i]=="time" or types[i]=="timestamp":
            #     print("x")
    return df

# print(Load_Functions.get_data("AdventureWorks2019-HumanResources-Shift").info())

# print(get_columns_name("DWH_WITH_PYHTON-dbo-Shift"))

# print(cast_data_types("AdventureWorks2019-HumanResources-Shift","DWH_WITH_PYHTON-dbo-Shift"))

# print(get_columns_type("DWH_WITH_PYHTON-dbo-Shift"))