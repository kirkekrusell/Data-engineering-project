# 1. Initialize the Environment

```shell
docker-compose up -d
```

### Credentials

| Component    | Username           | Password     | Port  |
|--------------|--------------------|--------------|-------|
| airflow-db   | airflow            | airflow      | 5432  |
| prices-db    | prices_user        | prices_pass  | 5433  |
| pgAdmin      | admin@example.com  | admin        | 5050  |
| Clickhouse   | default            | clickhouse   | 8123  |

Access pgAdmin at:
http://localhost:5050

Access Airflow at:
http://localhost:8180

Access Clickhouse at:
http://localhost:8123

# DAG - MTR file quality check

The DAG (`validate.py`) is located in the `implementation/` folder. When setting up Airflow, you need to copy this file into the Airflow DAGs directory:

`cp implementation/validate.py airflow/dags/`

First time it does not automatically detect the dag, you need to run airflow init.

`airflow db init`


## DAG overview

This DAG reads the modified MTR file (`mtr_test_2.csv`), checks for NAs in the "Registrikood" column, removes the found rows with NAs and creates a new file version. The dag runs once a week at midnight on Sunday morning.

## Data Storage (ClickHouse)  
# Bronze level

In CLickHouse Query create table bronze_mtr_raw where we are adding new data
create_table_query = """
CREATE TABLE IF NOT EXISTS bronze_mtr_raw (
    registrikood String,
    tegevusala String,
    alguskuupaev Date,
    loppkuupaev Date,
    staatus String,
    allikas String
) ENGINE = MergeTree()
ORDER BY registrikood;
"""

The DAG (`load_to_clickhouse.py`) is located in the `implementation/` folder. When setting up Airflow, you need to copy this file into the Airflow DAGs directory:

`cp implementation/load_to_clickhouse.py airflow/dags/`

# Silver level

Create the Silver Model

This model will clean and filter your raw data from the Bronze layer.
SELECT
    registrikood,
    lower(tegevusala) AS tegevusala,
    alguskuupaev,
    loppkuupaev,
    staatus,
    allikas
FROM {{ ref('bronze_mtr_raw') }}
WHERE staatus = 'aktiivne'

