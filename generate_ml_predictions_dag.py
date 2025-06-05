from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np
import psycopg2
import joblib
import json
import os
import boto3
import tempfile

def generate_predictions():
    # --- S3 path for the model ---
    s3_client = boto3.client('s3')
    bucket_name = 'sp-classifier-mwaa-2'
    key = 'trained model/model_Train_2020-2022_Test_2023-2025.pkl'

    with tempfile.NamedTemporaryFile() as tmp_file:
        s3_client.download_file(bucket_name, key, tmp_file.name)
        loaded = joblib.load(tmp_file.name)

    model = loaded['model']
    feature_cols = loaded['features']

    # --- Postgres connection ---
    conn = psycopg2.connect(
        host='sthub.c3uguk04fjqb.ap-southeast-2.rds.amazonaws.com',
        database='postgres',
        user='stpostgres',
        password='stocktrader',
        port=5432
    )
    cur = conn.cursor()

    # --- Fetch data ---
    df = pd.read_sql("""
        SELECT id, signal_data
        FROM stocktrader.leads_1
        WHERE signal_data IS NOT NULL AND prediction_train_2023_signal_data IS NULL
        ORDER BY lead_date;
    """, conn)

    # --- Parse JSON ---
    def extract_features(data):
        try:
            if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                return data[0]
        except Exception:
            pass
        return {}

    signal_features = df['signal_data'].apply(extract_features)
    feature_df = pd.DataFrame(signal_features.tolist())
    feature_df['id'] = df['id']

    # --- Drop missing rows and match columns ---
    feature_df = feature_df.dropna(subset=feature_cols)
    X = feature_df[feature_cols]

    # --- Predict ---
    preds = model.predict(X)
    feature_df['prediction'] = preds

    # --- Update database with predictions ---
    for idx, (_, row) in enumerate(feature_df.iterrows()):
        cur.execute("""
            UPDATE stocktrader.leads_1
            SET prediction_train_2023_signal_data = %s
            WHERE id = %s
        """, (int(row['prediction']), int(row['id'])))
        if idx % 100 == 0:
            conn.commit()
            print(f"Committed {idx} rows so far.")
    conn.commit()

    cur.close()
    conn.close()
    print("All predictions updated successfully.")

# --- Airflow DAG ---
from airflow.models import Variable

with DAG(
    dag_id='generate_ml_predictions_dag',
    start_date=days_ago(1),
    schedule_interval=None, 
    catchup=False,
    tags=['ml', 'predictions']
) as dag:

    run_prediction_task = PythonOperator(
        task_id='generate_predictions',
        python_callable=generate_predictions
    )
