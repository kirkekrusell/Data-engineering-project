from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from clickhouse_driver import Client
import pandas as pd
import os

def load_csv_to_clickhouse(**context):
    date_str = context['ds']  # Airflowi execution_date (YYYY-MM-DD)
    file_path = f"/opt/airflow/data/MTR_{date_str}.csv"

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV fail puudub: {file_path}")

    df = pd.read_csv(file_path)

    client = Client(
        host='clickhouse',
        user='default',
        database='default'
    )

    client.execute(f"""
        ALTER TABLE bronze_mtr_raw DELETE WHERE allikas = 'MTR_{date_str}.csv'
    """)

    client.execute(
        'INSERT INTO bronze_mtr_raw (registrikood, tegevusala, alguskuupaev, loppkuupaev, staatus, allikas) VALUES',
        df.to_dict('records')
    )

dag = DAG(
    dag_id="load_csv_to_clickhouse",
    start_date=days_ago(1),
    schedule_interval="0 0 * * 0",
    catchup=False,
    max_active_runs=1
)

with dag:
    load_task = PythonOperator(
        task_id="load_csv_to_clickhouse",
        python_callable=load_csv_to_clickhouse,
        provide_context=True
    )

globals()["load_csv_to_clickhouse"] = dag

