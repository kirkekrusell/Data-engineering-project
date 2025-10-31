import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def validate_csv(data = "data/mtr_test_2.csv"):

    date_str = datetime.now().strftime("%Y-%m-%d")

    #reading CSV files
    df = pd.read_csv(data, sep=";", engine="python", dtype=str, encoding='latin2', on_bad_lines='skip')
    df.columns = df.columns.str.strip().str.replace('\u00a0', ' ').str.replace('\n', '').str.replace('\r', '')
    logging.info("Veerunimed: %s", df.columns.tolist())

    # column to check
    veerg = 'Registrikood'
    #print(df.columns)
    
    # replace empty/whitespace-only strings with NaN for all object columns
    df = df.replace(r'^\s*$', np.nan, regex=True)

    # find and print rows with missing values in the specified column
    missing = df[df[veerg].isna()]
    print(len(missing))
    print(missing.head())

    #drop rows that have ANY NaN in column 'Registrikood'
    df = df.dropna(subset=[veerg]).reset_index(drop=True)
    
    #rename columns for ClickHouse
    df = df.rename(columns={'Registrikood': 'registrikood','Tegevusala': 'tegevusala','Kehtivuse algus': 'alguskuupaev','Kehtivuse lopp': 'loppkuupaev','Kehtiv': 'staatus','Lisainfo': 'allikas'})
    df['allikas'] = f"MTR_{date_str}.csv"
    
    #choose only important columns
    df = df[['registrikood', 'tegevusala', 'alguskuupaev', 'loppkuupaev', 'staatus', 'allikas']]
    print("Uued veerunimed:", df.columns.tolist())
    df.to_csv(f'data/MTR_{date_str}.csv', index=False)



with DAG(
    dag_id="validate_csv",
    start_date=days_ago(1),
    schedule_interval="0 0 * * 0",  # once a week at midnight on Sunday morning
    catchup=False,
    max_active_runs=1
) as dag:

    fetch_task = PythonOperator(
        task_id="validate_csv",
        python_callable=validate_csv,
        provide_context=True
    )
