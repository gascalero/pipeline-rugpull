from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    '2-Cargar_data',
    default_args=default_args,
    description='DAG para cargar datos desde el servidor a PostgreSQL sin preprocesamiento',
    schedule_interval='0 0 * * *',  # Solo ejecución manual si es "None"
    start_date=datetime(2025, 3, 30, 0, 0, 0),
    catchup=False,
    max_active_runs=1
)

database_name = 'airflow'  # Name of the database (used as schema if needed)
table_name = 'covertype'

# PostgreSQL CREATE TABLE SQL (adjusted for PostgreSQL)
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    id SERIAL PRIMARY KEY,
    Elevation INT NOT NULL, 
    Aspect INT NOT NULL, 
    Slope INT NOT NULL, 
    Horizontal_Distance_To_Hydrology INT NOT NULL,
    Vertical_Distance_To_Hydrology INT NOT NULL,
    Horizontal_Distance_To_Roadways INT NOT NULL,
    Hillshade_9am INT NOT NULL,
    Hillshade_Noon INT NOT NULL,
    Hillshade_3pm INT NOT NULL,
    Horizontal_Distance_To_Fire_Points INT NOT NULL, 
    Wilderness_Area VARCHAR(50) NOT NULL,  
    Soil_Type VARCHAR(50) NOT NULL,  
    Cover_Type INT NOT NULL
);
"""

def server_response(group_number):
     server_url = 'http://10.43.101.202:80/data'
     server_url_restart = 'http://10.43.101.202:80/restart_data_generation'
    
     params = {"group_number": group_number}

     response = requests.get(server_url_restart, params=params)

     response = requests.get(server_url, params=params)

     return response



def load_data(**kwargs):
    excecutions = 9


    for i in range(1,excecutions + 1):
        print(i)
        print(f"Ejecutando iteración {i}")
        raw = server_response(i)
        print(raw.content.decode('utf-8'))
        data = json.loads(raw.content.decode('utf-8'))
        
        df = pd.DataFrame(data["data"], columns=[
            "Elevation", "Aspect", "Slope", 
            "Horizontal_Distance_To_Hydrology", "Vertical_Distance_To_Hydrology",
            "Horizontal_Distance_To_Roadways", "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
            "Horizontal_Distance_To_Fire_Points", "Wilderness_Area", "Soil_Type", "Cover_Type"
        ])

        num_cols = df.columns.difference(["Wilderness_Area", "Soil_Type"])
        df[num_cols] = df[num_cols].astype(int)

        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = postgres_hook.get_sqlalchemy_engine()

        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {database_name};"
        postgres_hook.run(create_schema_sql)

        df.to_sql(
            name=table_name,
            con=engine,
            schema=database_name,
            if_exists='append',
            index=False,
            chunksize=1000
        )


        count_query = f"SELECT COUNT(*) FROM {database_name}.{table_name}"
        records_count = postgres_hook.get_records(count_query)[0][0]
        print(f"Filas cargadas: {records_count}")
        time.sleep(5)
    return "continue"

 


def decide_next_task(**kwargs):
    iter_count = Variable.get("dag_iter_count", default_var=1)
    iter_count = int(iter_count)
    time.sleep(5)
    if iter_count > 10:
        return "stop_task"
    else:
        return "load_data"

branch_task = BranchPythonOperator(
    task_id="decide_next_task",
    python_callable=decide_next_task,
    provide_context=True,
    dag=dag,
)

stop_task = PythonOperator(
    task_id="stop_task",
    python_callable=lambda: print("DAG detenido tras 10 iteraciones"),
    dag=dag
)

# PostgreSQL Operator to create the table (with the correct schema handling)
create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',  # PostgreSQL connection ID
    sql=create_table_sql,
    dag=dag
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

create_table_task >> branch_task >> [load_data_task, stop_task]