# Data Engineering Project – Business Activity Analysis
# Overview
This project analyzes Estonian business registry data and activity records to build a multi-layered data pipeline using Airflow, ClickHouse, and dbt. The pipeline classifies business activities by risk level and transforms raw data into clean, structured dimensional models for analytical use.

# 1. Environment Setup
```shell
docker-compose up -d
```

### Credentials

| Component    | Username           | Password     | Port  |
|--------------|--------------------|--------------|-------|
| airflow-db   | airflow            | airflow      | 5432  |
| pgAdmin      | admin@example.com  | admin        | 5050  |
| Clickhouse   | airflow_user       | airflow_pass | 8123  |

Access pgAdmin at:
http://localhost:5050

Access Airflow at:
http://localhost:8180

Access Clickhouse at:
http://localhost:8123

#  Cleaning Business Registry Data (CSV)
## Python Script
Run the cleaning script:
```bash
python clean_csv.py
```
This script:
    reads the raw business registry CSV,
    fixes date formats,
    removes problematic characters (quotes, commas, semicolons),
    selects only the required 8 columns:
        name, registry_code, vat_code, initial_registration_date, normalized_address, postal_code, legal_form, legal_form_subtype.

## Load into ClickHouse
```bash
docker cp "data/ettevotjad_clean.csv" clickhouse:/tmp/ettevotjad_clean.csv
docker exec -it clickhouse bash
clickhouse-client --query="TRUNCATE TABLE raw_company_data"
clickhouse-client --query="INSERT INTO raw_company_data FORMAT CSVWithNames SETTINGS format_csv_delimiter=';'" < /tmp/ettevotjad_clean.csv
clickhouse-client --query="SELECT COUNT(*) FROM raw_company_data"
```

Table Definition
```bash
CREATE TABLE IF NOT EXISTS raw_company_data (
    name String,
    registry_code String,
    vat_code String,
    initial_registration_date Date,
    normalized_address String,
    postal_code String,
    legal_form String,
    legal_form_subtype String
) ENGINE = MergeTree
ORDER BY registry_code;
```
You can also do it in terminal
```bash
docker exec -it clickhouse bash
```
Start the ClickHouse SQL client
```bash
clickhouse-client
```
Now you’ll see the ClickHouse prompt: `clickhouse :)`

Paste the SQL to create the table
```bash
CREATE TABLE IF NOT EXISTS bronze_mtr_raw (
    registrikood String,
    tegevusala String,
    alguskuupaev Date,
    loppkuupaev Date,
    staatus String,
    allikas String
) ENGINE = MergeTree
ORDER BY registrikood;
```
Press Enter — you should see:
```bash
CREATE TABLE bronze_mtr_raw
Ok.
```
Verify the table was created
```bash
SHOW TABLES;
SELECT * FROM bronze_mtr_raw LIMIT 5;
DESCRIBE TABLE bronze_mtr_raw;
```

# Airflow DAGs
## DAG: validate.py - MTR file quality check
Validates the MTR file by removing rows with missing registry codes. The DAG (`validate.py`) is located in the `implementation/` folder. When setting up Airflow, you need to copy this file into the Airflow DAGs directory:
```bash
cp implementation/validate.py airflow/dags/
docker exec -it airflow-webserver bash
airflow db init
```
### DAG overview
This DAG reads the modified MTR file (`mtr_test_2.csv`), checks for NAs in the "Registrikood" column, removes the found rows with NAs and creates a new file version. The dag runs once a week at midnight on Sunday morning.

## DAG: load_to_clickhouse.py
Install ClickHouse driver in Airflow containers before loading dag:

```bash 
docker exec -it airflow-webserver bash
pip install clickhouse-driver
exit

docker exec -it airflow-scheduler bash
pip install clickhouse-driver
exit
```

Loads cleaned CSV data into ClickHouse.
```bash
cp implementation/load_to_clickhouse.py airflow/dags/
```
### DAG overview
Bronze – Raw CSV data from Airflow to ClickHouse
Silver – dbt cleaned tables (status=active)
Gold – dbt star schema (fact + 3 dimension)

# Bronze Layer – Raw MTR Data
In CLickHouse Query create table bronze_mtr_raw where we are adding new data
For MTR
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
You created a raw table bronze_mtr_raw in ClickHouse to store activity data from the MTR file. This table includes: registry_code, activity_area, start_date, end_date, status, source

The DAG (`load_to_clickhouse.py`) is located in the `implementation/` folder. When setting up Airflow, you need to copy this file into the Airflow DAGs directory:

Load DAG
```cp implementation/load_to_clickhouse.py airflow/dags/```

Verify in ClickHouse:

`docker exec -it clickhouse clickhouse-client`

```bash
SHOW TABLES;
SELECT * FROM bronze_mtr_raw LIMIT 10;
DESCRIBE TABLE bronze_mtr_raw;
```
<img width="1279" height="353" alt="image" src="https://github.com/user-attachments/assets/cc5bae6f-9e1b-4ab9-8992-53cb85e292c6" />

# dbt Transformations

## Dockerfile for dbt - Dockerfile
Create this file "Dockerfile" in notebook and change it so it does not have a file type in Data-engineering-project/ and paste:

```bash
FROM python:3.11-slim
RUN apt-get update && apt-get install -y build-essential git curl
RUN pip install dbt-core dbt-clickhouse
WORKDIR /dbt
ENTRYPOINT ["dbt"]
```
## dbt Project Configuration - dbt_project.yml
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
      +tags: ["silver"]
    gold:
      +materialized: table
      +tags: ["gold"]
```
Save to C:\Users\user\.dbt\profiles.yml

## profiles.yml
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

# Build and run dbt container:
Open PowerShell/Terminal in Data-engineering-project and run:
`docker build -t my-dbt-clickhouse`
NB in thisone you should change youruser to your personal username
```bash
docker run -it --rm `
  -v ${PWD}:/dbt `
  -v C:\Users\youruser\.dbt:/root/.dbt `
  --workdir /dbt `
  --entrypoint bash `
  my-dbt-clickhouse
```
Once you are in you should see something like this: root@cbe5417dfda7:/dbt#
Inside container:
```bash
dbt debug
dbt run --select silver_mtr_clean
dbt test
```

# Silver Layer – Cleaned MTR Data
## Model: silver_mtr_clean.sql
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
Save to: models/silver/silver_mtr_clean.sql

## Schema: models/silver/schema.yml
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

# Gold Layer
This folder contains the gold models for the project, representing the final curated layer of the analytics warehouse. These models are designed for business intelligence, reporting, and downstream analysis.
## Fact table - fact_activity_event.sql
Location models/gold/fact_activity_event.sql
```bash
SELECT
    concat(registrikood, '_', alguskuupaev) AS event_id,
    c.company_id,
    a.activity_type_id,
    s.status_code AS status,
    m.alguskuupaev AS start_date,
    m.loppkuupaev AS end_date,
    toYYYYMMDD(m.alguskuupaev) AS start_date_id,
    toYYYYMMDD(m.loppkuupaev) AS end_date_id,
    dateDiff('day', m.alguskuupaev, m.loppkuupaev) AS duration_days,
    a.risk_level
FROM {{ ref('silver_mtr_clean') }} m
LEFT JOIN {{ ref('dim_company') }} c ON m.registrikood = c.registry_code
LEFT JOIN {{ ref('dim_activity_type') }} a ON lower(m.tegevusala) = lower(a.activity_area)
LEFT JOIN {{ ref('dim_status') }} s ON m.staatus = s.status_code
```
## Dimension – dim_company.sql (SCD Type 2)
Location: models/gold/dim_company.sql
```bash
SELECT
    ariregistri_kood AS registry_code,
    ariregistri_kood AS company_id,
    nimi AS company_name,
    kmkr_nr AS vat_code,
    ettevotja_esmakande_kpv AS initial_registration_date,
    ads_normaliseeritud_taisaadress AS normalized_address,
    indeks_ettevotja_aadressis AS postal_code,
    ettevotja_oiguslik_vorm AS legal_form,
    ettevotja_oigusliku_vormi_alaliik AS legal_form_subtype,
    toDate('2025-01-01') AS valid_from,
    toDate('9999-12-31') AS valid_to
FROM {{ source('default', 'raw_company_data') }}
GROUP BY ariregistri_kood, nimi, kmkr_nr, ettevotja_esmakande_kpv,
         ads_normaliseeritud_taisaadress, indeks_ettevotja_aadressis,
         ettevotja_oiguslik_vorm, ettevotja_oigusliku_vormi_alaliik
```
## Dimension – dim_date.sql (SCD Type 0)
location: models/gold/dim_date.sql
```bash
SELECT
    toYYYYMMDD(date) AS date_id,
    date,
    toDayOfWeek(date) AS day_of_week,         -- 1 = Monday, 7 = Sunday
    toMonth(date) AS month_number,            -- 1–12
    toQuarter(date) AS quarter,               -- 1–4
    NOT toDayOfWeek(date) IN (6, 7) AS is_weekday,
    toWeek(date) AS week_number
FROM (
    SELECT
        addDays(toDate('2025-01-01'), number) AS date
    FROM system.numbers
    LIMIT 365
)
```

## Dimension – dim_activity_type.sql (SCD Type 0)
Location: models/gold/dim_activity_type.sql
```bash
SELECT
    rowNumberInAllBlocks() AS activity_type_id,
    lower(tegevusala) AS activity_area,
    'N/A' AS additional_info,
    CASE
        WHEN lower(tegevusala) LIKE '%finants%' THEN 'high'
        WHEN lower(tegevusala) LIKE '%ehitus%' THEN 'medium'
        ELSE 'low'
    END AS risk_level
FROM {{ ref('silver_mtr_clean') }}
GROUP BY tegevusala
```
## Dimension – dim_status.sql
```bash
SELECT
    staatus AS status_code,
    staatus AS status_label
FROM {{ ref('silver_mtr_clean') }}
GROUP BY staatus
```

## Schema - models/gold/schema.yml
```bash
version: 2

models:
  - name: fact_activity_event
    description: "Fact table for activity events"
    columns:
      - name: event_id
        tests: [unique, not_null]
      - name: company_id
        tests: [not_null]
      - name: activity_type_id
        tests: [not_null]
      - name: status
        tests: [not_null]
      - name: duration_days
        description: "Duration in days"
      - name: risk_level
        description: "Risk classification"

  - name: dim_company
    description: "Company dimension with SCD Type 2"
    columns:
      - name: company_id
        tests: [unique, not_null]
      - name: valid_from
        tests: [not_null]
      - name: valid_to
        tests: [not_null]

  - name: dim_date
    description: "Date dimension for calendar analysis"
    columns:
      - name: date_id
        tests: [unique, not_null]

  - name: dim_activity_type
    description: "Activity type dimension with risk classification"
    columns:
      - name: activity_type_id
        tests: [unique, not_null]

  - name: dim_status
    description: "Status dimension"
    columns:
      - name: status_code
        tests: [unique, not_null]
```
## raw_sources.yml
```bash
version: 2

sources:
  - name: default
    tables: 
      - name: raw_company_data
```

Add them to models/gold/ and then run
```bash
dbt run --select gold
dbt test --select gold
```
# Orchestration – Airflow + dbt



