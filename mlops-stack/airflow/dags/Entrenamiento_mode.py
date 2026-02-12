from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import numpy as np
import joblib
from sklearn.metrics import accuracy_score
from sklearn.pipeline import Pipeline
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LogisticRegression, LogisticRegressionCV
from sklearn.compose import ColumnTransformer
import os
from datetime import datetime, timedelta
import psycopg2
from sklearn.model_selection import train_test_split
import os
import mlflow
import requests
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import make_column_transformer
from sklearn.model_selection import GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    '4-Entrenamiento_model',
    default_args=default_args,
    description='DAG para entrenar modelos de machine learning',
    schedule_interval='2 0 * * *',
    start_date=datetime(2025, 3, 30,0,2,0),
    catchup=False
)

processed_dir = '/opt/airflow/data/processed_data'
models_dir = '/opt/airflow/models'


# Conexión a la base de datos PostgreSQL
def get_postgres_connection():
    conn = psycopg2.connect(
        host='10.43.101.202',         # Nombre del servicio de PostgreSQL en Docker
        port='5432',             # Puerto predeterminado de PostgreSQL
        user='airflow',          # Usuario de PostgreSQL
        password='airflow',      # Contraseña de PostgreSQL
        database='airflow'    # Nombre de la base de datos
    )
    return conn

# Query the covertype table and return a DataFrame
def query_covertype():
    query = "SELECT * FROM covertype limit 1000"
    conn = get_postgres_connection()
    
    # Use pandas to execute the query and store the result in a DataFrame
    df = pd.read_sql(query, conn)
    
    # Close the connection
    conn.close()

    # Return the DataFrame
    return df

def entrenar_modelo(**kwargs):
    df_covertype = query_covertype()

    # Assuming df_covertype is your DataFrame


    # Split the dataframe into features (X) and target (y)
    # Assuming the target variable is in a column called 'cover_type'
    X = df_covertype.drop(columns=['Cover_Type'])  # Features
    y = df_covertype['Cover_Type']  # Target variable

    # Perform the train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # X_train, X_test will now contain the feature data for training and testing
    # y_train, y_test will contain the target labels for training and testing
    column_trans = make_column_transformer((OneHotEncoder(handle_unknown='ignore'),
                                        ["Wilderness_Area", "Soil_Type"]),
                                      remainder='passthrough') # pass all the numeric values through the pipeline without any changes.
    pipe = Pipeline(steps=[("column_trans", column_trans),("scaler", StandardScaler(with_mean=False)), ("RandomForestClassifier", RandomForestClassifier())])
    param_grid =  {'RandomForestClassifier__max_depth': [1,2,3,10], 'RandomForestClassifier__n_estimators': [10,11]}

    search = GridSearchCV(pipe, param_grid, n_jobs=2)
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = "http://10.43.101.202:9000"
    os.environ['AWS_ACCESS_KEY_ID'] = 'admin'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'supersecret'

    # connect to mlflow
    mlflow.set_tracking_uri("http://10.43.101.202:5000")
    mlflow.set_experiment("mlflow_tracking_examples")

    mlflow.sklearn.autolog(log_model_signatures=True, log_input_examples=True, registered_model_name="modelo1")

    with mlflow.start_run(run_name="autolog_pipe_model_reg") as run:
        search.fit(X_train, y_train)

    return 0


entrenar_modelo_task = PythonOperator(
    task_id='entrenar_modelo',
    python_callable=entrenar_modelo,
    provide_context=True,
    dag=dag
)

entrenar_modelo_task