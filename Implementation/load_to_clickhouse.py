from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from clickhouse_driver import Client
from datetime import datetime, date
import pandas as pd
import os

def load_csv_to_clickhouse(**context):
    date_str = context['ds']  # Airflow execution_date (YYYY-MM-DD)
    file_path = f"/tmp/MTR_{date_str}.csv"

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV fail puudub: {file_path}")

    # Loe CSV ja ühtlusta veerunimed
    df = pd.read_csv(file_path, dtype=str)
    df.columns = df.columns.str.lower().str.strip()

    # Nimeta vajalikud veerud ümber ClickHouse'i jaoks
    df = df.rename(columns={
        'registrikood': 'registrikood',
        'tegevusala': 'tegevusala',
        'kehtivuse algus': 'alguskuupaev',
        'kehtivuse lopp': 'loppkuupaev',
        'kehtiv': 'staatus'
    })

    # Kuupäevade teisendus
    def parse_loppkuupaev(value):
        if isinstance(value, str):
            value = value.strip().lower()
            if "tähtajatu" in value:
                return date(9999, 12, 31)
            try:
                parsed = datetime.strptime(value, "%d.%m.%Y")
                return parsed.date()
            except ValueError:
                return date(1900, 1, 1)  # tähis vigase või puuduva kuupäeva jaoks
        return date(1900, 1, 1)

    df['loppkuupaev'] = df['loppkuupaev'].apply(parse_loppkuupaev)

    df['alguskuupaev'] = pd.to_datetime(df['alguskuupaev'], format="%d.%m.%Y", dayfirst=True, errors='coerce')
    df['alguskuupaev'] = df['alguskuupaev'].apply(lambda x: x.date() if pd.notnull(x) else date(1900, 1, 1))

    # Lisa allikas
    df['allikas'] = f"MTR_{date_str}.csv"

    # Kontrolli, et kõik vajalikud veerud on olemas
    required = ['registrikood', 'tegevusala', 'alguskuupaev', 'loppkuupaev', 'staatus', 'allikas']
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Puuduvad veerud: {missing}")

    # Ühenda ClickHouse'iga
    client = Client(
        host='clickhouse',
        user='airflow_user',
        password='airflow_pass',
        database='default'
    )

    # Kustuta olemasolevad kirjed sama allikaga
    client.execute(f"""
        ALTER TABLE bronze_mtr_raw DELETE WHERE allikas = 'MTR_{date_str}.csv'
    """)

    # Lae uued kirjed
    client.execute(
        'INSERT INTO bronze_mtr_raw (registrikood, tegevusala, alguskuupaev, loppkuupaev, staatus, allikas) VALUES',
        df.to_dict('records')
    )

# Defineeri DAG
dag = DAG(
    dag_id="load_csv_to_clickhouse",
    start_date=days_ago(1),
    schedule_interval="0 0 * * 0",  # kord nädalas pühapäeva öösel
    catchup=False,
    max_active_runs=1
)

# Lisa ülesanne DAG-i
with dag:
    load_task = PythonOperator(
        task_id="load_csv_to_clickhouse",
        python_callable=load_csv_to_clickhouse,
        provide_context=True
    )

globals()["load_csv_to_clickhouse"] = dag



