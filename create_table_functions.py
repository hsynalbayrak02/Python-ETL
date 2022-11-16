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
###Column Infos for create temp table,cdc method
def get_column_infos(index,table_name):
    columns_json=open("Table_Columns_Infos/"+table_name+".json","r")
    json_data=json.load(columns_json)
    column_name=json_data["table_columns"][index]["name"]
    column_type = json_data["table_columns"][index]["type"]
    column_mandatory = json_data["table_columns"][index]["mode"]
    return column_name,column_type,column_mandatory

##column count we are using this in loop to create table for cdc method
def get_column_count(table_name):
    columns_json=open("Table_Columns_Infos/"+table_name+".json","r")
    json_data=json.load(columns_json)
    column_count=len(json_data["table_columns"])
    return column_count

