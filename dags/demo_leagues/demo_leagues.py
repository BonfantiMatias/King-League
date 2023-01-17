import json
from datetime import datetime, timedelta
import os
import logging
import airflow
from airflow.models import Variable
from airflow import models
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
import snowflake.connector as sf
import pandas as pd
import time
import random
import os
from datetime import datetime




default_arguments = {   'owner': 'Matias',
                        'email': 'matiasbonfanti87@gmail.com',
                        'retries':1 ,
                        'retry_delay':timedelta(minutes=5)}


with DAG('FOOTBAL_LEAGUES',
         default_args=default_arguments,
         description='Extracting Data Footbal League' ,
         start_date = datetime(2022, 9, 21),
         schedule_interval = None,
         tags=['tabla_espn'],
         catchup=False) as dag :


         params_info = Variable.get("feature_info", deserialize_json=True)
         url = ['https://www.resultados-futbol.com/kings-league']

         liga = ['Kings League']

         df_liga = {'LIGA':liga,
           'URL':url
          }

         df = pd.DataFrame(df_liga)
        
         
         df_team = pd.read_csv('/opt/airflow/dags/equipos.csv')
        
         def get_data():
    
            
            df = pd.read_html('https://www.resultados-futbol.com/kings-league')
            df = df[14]
            df = pd.DataFrame(df)
            df = df[df.columns[1:9]]
            df = df.rename(columns={'Equipos.1':'Equipos'})
            df["Equipos"] = df["Equipos"].apply(lambda x: x.replace("*", "") if x.find("*") != -1 else x)
            df['LIGA'] = 'Kings League'

            run_date = datetime.now()
            run_date = run_date.strftime("%Y-%m-%d")
            df['CREATED_AT'] = run_date

            return df

         def extract_info(df_team ,**kwargs):

            df_data = get_data()

            df_final = pd.merge(df_data,df_team,how='inner',on='Equipos')
            df_final = df_final[['ID','Equipos', 'Puntos','J.','G.','E.','P.','F.','C.','LIGA',
                'CREATED_AT']]

            df_final.to_csv('./premier_positions.csv',index=False)


         extract_data = PythonOperator(task_id='EXTRACT_FOTBALL_DATA',
                                    provide_context=True,
                                    python_callable=extract_info,
                                    op_kwargs={"df_team":df_team})

         upload_stage = SnowflakeOperator(

                    task_id='upload_data_stage',
                    sql='./queries/upload_stage.sql',
                    snowflake_conn_id='snow_new',
                    warehouse=params_info["DWH"],
                    database=params_info["DB"],
                    role=params_info["ROLE"],
                    params=params_info
                    )
         ingest_table = SnowflakeOperator(

                    task_id='ingest_table',
                    sql='./queries/upload_table.sql',
                    snowflake_conn_id='snow_new',
                    warehouse=params_info["DWH"],
                    database=params_info["DB"],
                    role=params_info["ROLE"],
                    params=params_info
                    )

         extract_data >>  upload_stage >> ingest_table