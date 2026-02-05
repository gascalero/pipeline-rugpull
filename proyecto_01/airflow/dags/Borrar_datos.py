from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Definir los argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    '1-clean_data_dag',
    default_args=default_args,
    description='DAG para borrar todos los datos en la tabla covertype',
    schedule_interval='0 1 * * *',
    start_date=datetime(2025, 3, 30, 1, 0, 0),
    catchup=False
)

table_name = 'covertype'

def delete_all_data():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    query = f"DELETE FROM {table_name}"
    postgres_hook.run(query)
    print("Todas las filas han sido eliminadas de la tabla")

delete_data_task = PythonOperator(
    task_id='delete_data_task',
    python_callable=delete_all_data,
    dag=dag
)

delete_data_task
