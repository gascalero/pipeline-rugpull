"""
DAG: Experimentation Pipeline - Rug Pull Detection
Flujo: CSVs locales → Bronze (MinIO) → Silver (PostgreSQL) → Gold (PostgreSQL) → MLflow
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# ─── Parámetros configurables ────────────────────────────────────────────────
BLOCKSTUDY = 13220488
WINDOW = 6646

WETH_ADDRESS = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'

DATA_RAW = '/opt/airflow/plugins/data/raw'

MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'mlops')

PG_CONN = {
    'host': 'postgres',
    'port': 5432,
    'dbname': 'rugpull_db',  # ← cambio
    'user': 'airflow',
    'password': 'airflow',
}

MLFLOW_URI = 'http://mlflow:5000'


# ─── TASK 1: Bronze ──────────────────────────────────────────────────────────
def task_bronze():
    import pandas as pd
    import boto3
    import io

    print("📥 Leyendo CSVs locales...")
    df_pools = pd.read_csv(f'{DATA_RAW}/pool_list_complete.csv')
    df_pool_events = pd.read_csv(f'{DATA_RAW}/eventos_pool_sync_mint_burn.csv')
    df_tokens_metadata = pd.read_csv(f'{DATA_RAW}/token_metadata_complete.csv')
    df_token_transfers = pd.read_csv(f'{DATA_RAW}/eventos_transfers_tokens.csv', chunksize=500000)
    df_token_transfers = pd.concat(df_token_transfers, ignore_index=True)

    # Columnas WETH requeridas por feature_engineering y labeling
    df_pools['weth_is_token0'] = df_pools['token0'].str.lower() == WETH_ADDRESS.lower()
    df_pools['weth_is_token1'] = df_pools['token1'].str.lower() == WETH_ADDRESS.lower()

    datasets = {
        'bronze/pools.parquet': df_pools,
        'bronze/pool_events.parquet': df_pool_events,
        'bronze/tokens_metadata.parquet': df_tokens_metadata,
        'bronze/token_transfers.parquet': df_token_transfers,
    }

    s3 = boto3.client('s3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    existing_buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    if MINIO_BUCKET not in existing_buckets:
        s3.create_bucket(Bucket=MINIO_BUCKET)
        print(f"Bucket '{MINIO_BUCKET}' creado")

    for key, df in datasets.items():
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=buffer.getvalue())
        print(f"✅ {key} ({len(df)} filas)")

    print("✅ Bronze completo")


# ─── TASK 2: Silver ──────────────────────────────────────────────────────────
def task_silver():
    import sys
    sys.path.insert(0, '/opt/airflow/plugins')

    import pandas as pd
    import boto3
    import io
    import psycopg2
    from psycopg2.extras import execute_values
    from src.feature_engineering import compute_features

    s3 = boto3.client('s3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    def read_parquet(key):
        obj = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
        return pd.read_parquet(io.BytesIO(obj['Body'].read()))

    print("📥 Leyendo Bronze desde MinIO...")
    df_pools = read_parquet('bronze/pools.parquet')
    df_pool_events = read_parquet('bronze/pool_events.parquet')
    df_tokens_metadata = read_parquet('bronze/tokens_metadata.parquet')
    df_token_transfers = pd.read_csv(f'{DATA_RAW}/eventos_transfers_tokens.csv',
                                  chunksize=500000)
    df_token_transfers = pd.concat(df_token_transfers, ignore_index=True)

    print("⚙️ Computando features...")
    features_list = []
    for token in df_pools['token_address']:
        result = compute_features(
            token, df_pools, df_pool_events, df_token_transfers, df_tokens_metadata
        )
        if result is not None:
            features_list.append(result)

    df_silver = pd.DataFrame(features_list)
    print(f"✅ Features: {len(df_silver)} tokens")

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS silver_features')
    cur.execute('''
        CREATE TABLE silver_features (
            token_address       TEXT,
            n_syncs             INTEGER,
            "WETH"              DOUBLE PRECISION,
            prices              DOUBLE PRECISION,
            liquidity           DOUBLE PRECISION,
            num_transactions    INTEGER,
            n_unique_addresses  INTEGER,
            tx_curve            DOUBLE PRECISION,
            mints               INTEGER,
            burns               INTEGER,
            difference_token_pool INTEGER
        )
    ''')
    rows = [tuple(row) for row in df_silver.itertuples(index=False)]
    execute_values(cur, 'INSERT INTO silver_features VALUES %s', rows)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Silver guardado en PostgreSQL")


# ─── TASK 3: Gold ────────────────────────────────────────────────────────────
def task_gold():
    import sys
    sys.path.insert(0, '/opt/airflow/plugins')

    import pandas as pd
    import boto3
    import io
    import psycopg2
    from psycopg2.extras import execute_values
    from src.labeling import build_label_dataframe

    s3 = boto3.client('s3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    def read_parquet(key):
        obj = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
        return pd.read_parquet(io.BytesIO(obj['Body'].read()))

    print("📥 Leyendo Bronze desde MinIO...")
    df_pools = read_parquet('bronze/pools.parquet')
    df_pool_events = read_parquet('bronze/pool_events.parquet')
    df_token_transfers = pd.read_csv(f'{DATA_RAW}/eventos_transfers_tokens.csv',
                                  chunksize=500000)
    df_token_transfers = pd.concat(df_token_transfers, ignore_index=True)

    print("🏷️ Generando labels...")
    df_gold = build_label_dataframe(df_pools, df_pool_events, df_token_transfers, BLOCKSTUDY)
    print(f"✅ {len(df_gold)} tokens etiquetados")
    print(df_gold['fraud_type'].value_counts())

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS gold_labels')
    cur.execute('''
        CREATE TABLE gold_labels (
            token_address   TEXT,
            pair_address    TEXT,
            label           INTEGER,
            fraud_type      TEXT
        )
    ''')
    rows = [tuple(row) for row in df_gold.itertuples(index=False)]
    execute_values(cur, 'INSERT INTO gold_labels VALUES %s', rows)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Gold guardado en PostgreSQL")


# ─── TASK 4: Train ───────────────────────────────────────────────────────────
def task_train():
    import pandas as pd
    import psycopg2
    import mlflow
    import mlflow.xgboost
    import xgboost as xgb
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import roc_auc_score, classification_report

    conn = psycopg2.connect(**PG_CONN)
    df_features = pd.read_sql('SELECT * FROM silver_features', conn)
    df_labels = pd.read_sql('SELECT token_address, label FROM gold_labels', conn)
    conn.close()

    df = df_features.merge(df_labels, on='token_address', how='inner')
    print(f"✅ Dataset: {len(df)} tokens | {df['label'].value_counts().to_dict()}")

    exclude_cols = ['token_address', 'label']
    feature_cols = [c for c in df.columns if c not in exclude_cols]
    X = df[feature_cols].fillna(0)
    y = df['label']

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment('rug_pull_detection')

    with mlflow.start_run():
        mlflow.log_param('blockstudy', BLOCKSTUDY)
        mlflow.log_param('window', WINDOW)
        mlflow.log_param('n_tokens', len(df))
        mlflow.log_param('features', feature_cols)

        model = xgb.XGBClassifier(
            n_estimators=100,
            max_depth=4,
            eval_metric='logloss',
            random_state=42
        )
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])

        mlflow.log_metric('auc', auc)
        mlflow.xgboost.log_model(model, 'model', registered_model_name='rug_pull_xgboost')

        print(f"✅ AUC: {auc:.4f}")
        print(classification_report(y_test, y_pred))

    print("✅ Modelo registrado en MLflow")


# ─── Definición del DAG ──────────────────────────────────────────────────────
with DAG(
    dag_id='experimentation_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['experimentation', 'rug-pull'],
) as dag:

    bronze = PythonOperator(task_id='bronze', python_callable=task_bronze)
    silver = PythonOperator(task_id='silver', python_callable=task_silver)
    gold   = PythonOperator(task_id='gold',   python_callable=task_gold)
    train  = PythonOperator(task_id='train',  python_callable=task_train)

    bronze >> silver >> gold >> train