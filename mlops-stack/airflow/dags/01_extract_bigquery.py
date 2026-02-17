from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("¡Hola desde mi primer DAG!")
    print("Este es el pipeline de rug pulls")

with DAG(
    dag_id='01_test_extraction',
    start_date=datetime(2025, 2, 1),
    schedule_interval=None,  # Manual trigger
    catchup=False,
) as dag:
    
    task_hello = PythonOperator(
        task_id='hello',
        python_callable=hello_world
    )