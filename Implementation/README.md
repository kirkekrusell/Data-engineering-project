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

## 2. Data Storage (ClickHouse)  

```bash 
docker exec -it airflow-webserver bash
pip install clickhouse-driver
exit

docker exec -it airflow-scheduler bash
pip install clickhouse-driver
exit
```
# Bronze level

In CLickHouse Query create table bronze_mtr_raw where we are adding new data

```bash 
CREATE TABLE IF NOT EXISTS bronze_mtr_raw (
    registrikood String,
    tegevusala String,
    alguskuupaev Date,
    loppkuupaev Date,
    staatus String,
    allikas String
) ENGINE = MergeTree()
ORDER BY registrikood;
```

The DAG (`load_to_clickhouse.py`) is located in the `implementation/` folder. When setting up Airflow, you need to copy this file into the Airflow DAGs directory:

```cp implementation/load_to_clickhouse.py airflow/dags/```

Run the DAG in Airflow. Once successful, verify in ClickHouse:

`docker exec -it clickhouse clickhouse-client`

Example queries:
```bash
SHOW TABLES;
SELECT * FROM bronze_mtr_raw LIMIT 10;
DESCRIBE TABLE bronze_mtr_raw;
```
# 3. Transformation (dbt)
# Silver Layer – Cleaned Data (dbt)

Create a dbt model to clean and filter the raw data:

This model will clean and filter your raw data from the Bronze layer.
```bash
SELECT
    registrikood,
    lower(tegevusala) AS tegevusala,
    alguskuupaev,
    loppkuupaev,
    staatus,
    allikas
FROM {{ ref('bronze_mtr_raw') }}
WHERE staatus = 'aktiivne'
```
Save this as silver_mtr_clean.sql inside models/silver/

# Gold Layer – Analytical Model (dbt)
Create this file in Data-engineering-project/ and paste:
```bash
FROM python:3.11-slim
RUN apt-get update && apt-get install -y build-essential git curl
RUN pip install dbt-core dbt-clickhouse
WORKDIR /dbt
ENTRYPOINT ["dbt"]
```
2. dbt_project.yml
Create this file in Data-engineering-project/ and paste:
```bash
name: "data_engineering_project"
version: "1.0"
profile: "clickhouse_profile"
model-paths: ["models"]
target-path: "target"
clean-targets: ["target"]
models:
  data_engineering_project:
    silver:
      +materialized: table
```
3. silver_mtr_clean.sql
Place this file in: models/silver/silver_mtr_clean.sql
```bash
SELECT
    registrikood,
    lower(tegevusala) AS tegevusala,
    alguskuupaev,
    loppkuupaev,
    staatus,
    allikas
FROM {{ ref('bronze_mtr_raw') }}
WHERE staatus = 'aktiivne'
```
Schema.yml
Place this file in: models/silver/schema.yml
```bash
version: 2

models:
  - name: silver_mtr_clean
    description: "Cleaned MTR data with only active records"
    columns:
      - name: registrikood
        tests:
          - not_null
          - unique
      - name: tegevusala
        description: "Standardized activity name"
      - name: alguskuupaev
        tests:
          - not_null
```
profiles.yml
Create this file in: C:\Users\user\.dbt\profiles.yml
```bash
clickhouse_profile:
  target: dev
  outputs:
    dev:
      type: clickhouse
      schema: default
      host: localhost
      port: 8123
      user: airflow_user
      password: airflow_pass
      secure: false
      verify: false
      database: default
```
Build and Run Docker
Open PowerShell/Terminal in Data-engineering-project and run:
´docker build -t my-dbt-clickhouse .´

```bash
docker run -it --rm `
  -v ${PWD}:/dbt `
  -v C:\Users\lamps\.dbt:/root/.dbt `
  --workdir /dbt `
  --entrypoint bash `
  my-dbt-clickhouse
```
Once you are in you should see :

In the gold layer, build your dimensional model:
1 fact model (e.g. fact_mtr_activity)
3 dimension models (e.g. dim_company, dim_activity, dim_status)
1 test (e.g. unique or not null)
1 documentation file (.yml)
Use ref() to define dependencies between models

# Orchestration – Airflow + dbt

Integrate dbt into your Airflow DAG using BashOperator:
