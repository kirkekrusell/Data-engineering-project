from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.insert(0, '/opt/airflow/repo/Implementation/iceberg')

from bronze_mtr_iceberg_ingest import ingest_bronze_mtr

with DAG(
    'iceberg_bronze_ingest',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@weekly',
    catchup=False,
    tags=['iceberg', 'bronze'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_mtr_bronze',
        python_callable=ingest_bronze_mtr
    )
