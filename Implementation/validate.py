import pandas as pd
import numpy as np
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def validate_csv(data = "data/mtr_test_2.csv"):

    date_str = datetime.now().strftime("%Y-%m-%d")

    #reading CSV files
    df = pd.read_csv(data, sep=";", engine="python", dtype=str, encoding='latin2', on_bad_lines='skip')

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
    df.to_csv(f'data/MTR_{date_str}.csv', index=False)


with DAG(
    dag_id="validate_csv",
    start_date=days_ago,
    schedule_interval="0 0 * * 0",  # once a week at midnight on Sunday morning
    catchup=False,
    max_active_runs=1
) as dag:

    fetch_task = PythonOperator(
        task_id="validate_csv",
        python_callable=validate_csv,
        provide_context=True
    )
