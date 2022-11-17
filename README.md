# PyhtonETL
My source and target technologies are Sql server
I used AdventureWork2019 DB for source 

1-DB_Infos.json->Contains Server Names,DB Names,User,Passwords And Driver
2-DB_Types_Vs_Pyhton_Types.json->Mssql data types vs pyhton data types
3-Schedule_Types.txt->Daily,Monthly,Weekly etc.

***starting with AdventureWorks2019 is our source
***starting with DWH_WITH_PYHTON is our target in this project

4-AdventureWorks2019-HumanResources-Shift.json->Contains table informations like column name,type,nullable,pk column etc.
5-AdventureWorks2019-Person-Person.json->Contains table informations like column name,type,nullable,pk column etc.
6-AdventureWorks2019-Sales-Currency.json->Contains table informations like column name,type,nullable,pk column etc.
7-DWH_WITH_PYHTON-dbo-Currency.json->Contains table informations like column name,type,nullable,pk column etc.
8-DWH_WITH_PYHTON-dbo-Person.json->Contains table informations like column name,type,nullable,pk column etc.
9-DWH_WITH_PYHTON-dbo-Shift.json->Contains table informations like column name,type,nullable,pk column etc.
10-DWH_WITH_PYHTON-dm-currency_sumary.sql->Basic sql for dm mapping
11-DWH_WITH_PYHTON-dm-person_summary.sql->Basic sql for dm mapping

12-DM_Tables.json->Target Table Name,Load Type,Schedule Type,Status(Run or Not)
13-ODS_Tables.json->Source Table Name,Target Table Name,Load Type,Schedule Type,Status(Run or Not)

14-Cast_Data_Type.py->Mssql data types vs pyhton data types.I wrote cast codes but didn't use
15-create_table_functions.py->Some creating temp table functions for cdc method
16-etl_logs.py->For all logs
17-Load_Functions.py->The main file consolidate all codes and files
