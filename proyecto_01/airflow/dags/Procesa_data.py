import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.datasets import load_diabetes
from sklearn.ensemble import RandomForestRegressor
from sklearn.compose import make_column_transformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
import os
import random
import pandas as pd
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    '4-Procesa_data',
    default_args=default_args,
    description='DAG para experimentos en MLFLOW',
    schedule_interval='10 0 * * *',  # Solo ejecución manual si es "None"
    start_date=datetime(2025, 3, 30, 0, 10, 0),
    catchup=False,
    max_active_runs=1
)


os.environ['MLFLOW_S3_ENDPOINT_URL'] = "http://10.43.101.202:9000"
os.environ['AWS_ACCESS_KEY_ID'] = 'admin'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'supersecret'

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
    query = "SELECT * FROM covertype"
    conn = get_postgres_connection()
    
    # Use pandas to execute the query and store the result in a DataFrame
    df = pd.read_sql(query, conn)
    
    # Close the connection
    conn.close()

    # Return the DataFrame
    return df

def experimentar():
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

    mlflow.set_tracking_uri("http://10.43.101.202:5000")
    mlflow.set_experiment("mlflow_tracking_examples")

    mlflow.sklearn.autolog(log_model_signatures=True, log_input_examples=True, registered_model_name="modelo1")

    # Cambiar los hiperparámetros cada vez que se ejecute
    # n_estimators = random.randint(50, 200)
    # max_depth = random.randint(3, 10)
    # max_features = random.choice([2, 3, 4, 'auto'])

    with mlflow.start_run(run_name="autolog_pipe_model_reg") as run:
        # rf = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth, max_features=max_features)
        search.fit(X_train, y_train)
        mlflow.log_metric("r2_score", search.score(X_test, y_test))

    print("Experimento registrado correctamente.")


experimentos_task = PythonOperator(
    task_id='experimentos_task',
    python_callable=experimentar,
    dag=dag
)

experimentos_task