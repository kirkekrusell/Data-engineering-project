from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 11, 1),
    'retries': 1,
}

with DAG('dbt_gold_layer_refresh',
         default_args=default_args,
         schedule_interval='0 0 * * 0',  # Every Sunday at midnight
         catchup=False) as dag:

    dbt_run_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command='cd /dbt && dbt run --select gold'
    )

    dbt_test_gold = BashOperator(
        task_id='dbt_test_gold',
        bash_command='cd /dbt && dbt test --select gold'
    )

    dbt_run_gold >> dbt_test_gold
