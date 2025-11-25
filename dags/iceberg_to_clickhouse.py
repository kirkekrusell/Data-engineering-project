from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyiceberg.catalog.rest import RestCatalog
import pyarrow.parquet as pq
import s3fs
from clickhouse_driver import Client

# DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'iceberg_to_clickhouse',
    default_args=default_args,
    description='Load Iceberg bronze table into ClickHouse via MinIO Parquet',
    schedule_interval='0 0 * * 0',  # iga pühapäev keskööl
    start_date=datetime(2025, 11, 25),
    catchup=False
) as dag:

    def export_iceberg_to_s3():
        # Laadi Iceberg tabel
        catalog = RestCatalog(name="iceberg_rest", uri="http://iceberg-rest:8181")
        table = catalog.load_table("bronze.mtr_iceberg")
        arrow_table = table.scan().to_arrow()

        # Loo S3 ühendus
        fs = s3fs.S3FileSystem(
            key="minio_user",
            secret="minio_pass",
            client_kwargs={"endpoint_url": "http://minio:9000"}
        )

        # Salvesta Parquet MinIO-sse
        pq.write_table(
            arrow_table,
            "s3://warehouse/bronze/mtr_iceberg/mtr_iceberg.parquet",
            filesystem=fs
        )

    def create_clickhouse_table():
        client = Client(
            host='clickhouse',
            user='airflow_user',
            password='airflow_pass',
            database='default'
        )

        client.execute("""
        CREATE TABLE IF NOT EXISTS bronze_mtr_iceberg_ch (
            registrikood String,
            tegevusala String,
            alguskuupaev Date,
            loppkuupaev Date,
            staatus String,
            allikas String
        )
        ENGINE = S3(
            'http://minio:9000/warehouse/bronze/mtr_iceberg/mtr_iceberg.parquet',
            'minio_user',
            'minio_pass',
            'Parquet'
        )
        """)

    export_task = PythonOperator(
        task_id='export_iceberg_to_s3',
        python_callable=export_iceberg_to_s3
    )

    ch_task = PythonOperator(
        task_id='create_clickhouse_table',
        python_callable=create_clickhouse_table
    )

    export_task >> ch_task
