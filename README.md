# analytics_engineer_case
This Repo Contains a full analytics engineer pipeline for study.


The Challenge: https://github.com/rodmaraujo/analytics_engineer_case/blob/main/DENB_Challenge_AE_2025.pdf


### My Stack in this Project is:
- Docker (For environment)
  - Jupyter container (For python debug tests)
  - PostgreSQL container (DB for data modeling layers)
  - Superset Container (Dataviz)
  - Dagster Container (orchestration)
- Dbeaver(running pure SQL scripts v1)
- DBT(running SQL+Jinja Model Scripts)
- Power BI Desktop (Dataviz)
- ftpGrid (free FTP tests)

### Base Containers I used:

<img width="572" height="379" alt="Image" src="https://github.com/user-attachments/assets/8b1526f5-24ea-420e-9e6f-edba4bf67980" />


### Full Arch I developed:
<img width="2000" height="807" alt="Image" src="https://github.com/user-attachments/assets/4c1491e7-6202-42df-825e-ec33c5ad2342" />




# STEPS:
----------------------------------------------------------------------

###################################

### 1) Building my containers:
   
###################################

[PostgreSQL]     https://github.com/rodmaraujo/analytics_engineer_case/blob/main/postgresql-compose.yaml

[Jupyter]        https://github.com/rodmaraujo/analytics_engineer_case/blob/main/jupyter-lab-tests.yaml

[Dagster]        https://github.com/rodmaraujo/analytics_engineer_case/tree/main/dagster

[Superset]       https://github.com/rodmaraujo/analytics_engineer_case/blob/main/superset-compose.yaml

----------------------------------------------------------------------

###################################

### 2) Doing Python tests:
   
###################################

#### ----TASKs 1.a, 2.a.i of PDF case----

***Incremental Ingestion: If the raw tabela does not exists in PostgreSQL or if the CSV file date modified > raw ingestion_date field(I create this field in the scripts)

[Raw Ingestion]   https://github.com/rodmaraujo/analytics_engineer_case/blob/main/python_isolated_case_functions/1_source_to_postgresql_with_ingestion_date.py

<img width="1304" height="143" alt="Image" src="https://github.com/user-attachments/assets/1c2831d7-c95e-45c7-8d83-dfaf64e5410c" />


[Raw Table validate on Postgresql] https://github.com/rodmaraujo/analytics_engineer_case/blob/main/python_isolated_case_functions/2_loadsmart_raw_table_validate.py






#### ----TASK 2.a.ii, 2.a.iii, 2.b of PDF case----

*** Export the files in the sFTP every Dagster Daily Trigger

[Analysis Exports to sFTP and Email] https://github.com/rodmaraujo/analytics_engineer_case/blob/main/python_isolated_case_functions/3_export_reports_sftp_email.py

(Last script test! After the full Data Modeling Layers was built)

<img width="1485" height="215" alt="Image" src="https://github.com/user-attachments/assets/8ad5221b-2e0a-4da2-b1c5-1195ebd6f71f" />


----------------------------------------------------------------------

###################################

### 3) Building Data Modeling Layers:
   
###################################

#### ----TASKs 1.a, 2.b of PDF case----

#### [ Type 1: Pure SQL on PostgreSQL + Dbeaver ]  https://github.com/rodmaraujo/analytics_engineer_case/blob/main/Dimensional_Model_Script.sql

- You can just execute this full script on Dbeaver + PostgreSQL container connection and all layers will be created:
  
  -schemas:  
    [ loadsmart_dim ] [ loadsmart_fact ] [ loadsmart_anl ] [ loadsmart_reports ]

  -tables:
  
    loadsmart_dim.dim_pickup_location
  
    loadsmart_dim.dim_delivery_location
  
    loadsmart_dim.dim_sourcing_channel
  
    loadsmart_dim.dim_carrier
  
    loadsmart_dim.dim_shipper
  
    
    loadsmart_fact.fact_loadsmart_load
  
    loadsmart_anl.anl_pickup_location
  
    loadsmart_anl.anl_delivery_location
  
    loadsmart_anl.anl_carrier
  
    loadsmart_reports.loadsmart_id_lastmonth_report


#### [ Type 2: DBT Models ]  

If you prefer, drop all  modeling tables and just execute my DBT models:

DIM Models: https://github.com/rodmaraujo/analytics_engineer_case/tree/main/dbt_project/models/loadsmart/dimension

FACT Model: https://github.com/rodmaraujo/analytics_engineer_case/tree/main/dbt_project/models/loadsmart/fact

Star Analysis Models: https://github.com/rodmaraujo/analytics_engineer_case/tree/main/dbt_project/models/loadsmart/anl

Reports Models: https://github.com/rodmaraujo/analytics_engineer_case/tree/main/dbt_project/models/loadsmart/reports

You can just run "dbt build" on your dbt bash to build the complete Data Modeling.

##### Project Example: 

<img width="298" height="627" alt="Image" src="https://github.com/user-attachments/assets/7f6a90a5-60ef-4e90-aabf-ef0e4061b584" />

##### Dimensional Modeling DBT lineage:

<img width="1782" height="786" alt="Image" src="https://github.com/user-attachments/assets/3d436de2-60e5-4617-814c-11ac193e9d70" />

----------------------------------------------------------------------

###################################

### 4) The Dataviz Examples:
   
###################################


You can check my PBI and Superset Files in: https://github.com/rodmaraujo/analytics_engineer_case/tree/main/dataviz_loadsmart_ae_case

My simple Power BI dashboard testing the Data Models:

<img width="1453" height="819" alt="Image" src="https://github.com/user-attachments/assets/9a4e4599-ec70-4f57-93e8-e87f2ed16ec7" />

My Superset dashboard testing the Data Models:

![Image](https://github.com/user-attachments/assets/43ce73ac-4bc7-472e-b37f-727527b2d606)



----------------------------------------------------------------------

###################################

### 4) Orchestrating with Dagster:
   
###################################

All of the Raw Ingestion, Data Modeling and Export Pipeline could be Orchestrated with Dagster 

(I have been worked with Airflow during my career, but I wanted to test Dagster in this challenge only for curiosity and I liked! I'll explore more about him.)

<img width="393" height="2120" alt="Image" src="https://github.com/user-attachments/assets/6e77c711-f61f-446b-b857-9c079d687c03" />

<img width="393" height="426" alt="Image" src="https://github.com/user-attachments/assets/5adafed6-7561-4b0d-ab51-abad2420a294" />
<img width="784" height="37" alt="Image" src="https://github.com/user-attachments/assets/1a1e8666-a30c-41fe-b046-63738f195f7a" />

a) The Tasks have a dependency execution:  execute_ingestion_script  >> run_dbt_models >> export_reports_sftp

b) The Contract Schedule is Daily:7UTC




----------------

#### Hey! I hope you enjoyed my Analytics Engineer case! See ya!
