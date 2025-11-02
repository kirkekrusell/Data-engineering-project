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
| Clickhouse   | airflow_user       | airflow_pass | 8123  |

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
Install ClickHouse driver in Airflow containers:
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
#  Cleaning Register Data (CSV)

```bash
python clean_csv.py
docker cp "data/ettevotjad_clean.csv" clickhouse:/tmp/ettevotjad_clean.csv
docker exec -it clickhouse bash
clickhouse-client --query="TRUNCATE TABLE raw_company_data"
clickhouse-client --query="INSERT INTO raw_company_data FORMAT CSVWithNames SETTINGS format_csv_delimiter=';'" < /tmp/ettevotjad_clean.csv
clickhouse-client --query="SELECT COUNT(*) FROM raw_company_data"
```

```bash
CREATE TABLE raw_company_data (
    nimi String,
    ariregistri_kood String,
    ettevotja_oiguslik_vorm String,
    ettevotja_oigusliku_vormi_alaliik String,
    kmkr_nr String,
    ettevotja_staatus String,
    ettevotja_staatus_tekstina String,
    ettevotja_esmakande_kpv Date,
    ettevotja_aadress String,
    asukoht_ettevotja_aadressis String,
    asukoha_ehak_kood String,
    asukoha_ehak_tekstina String,
    indeks_ettevotja_aadressis String,
    ads_adr_id String,
    ads_ads_oid String,
    ads_normaliseeritud_taisaadress String,
    teabesysteemi_link String
) ENGINE = MergeTree
ORDER BY ariregistri_kood;
```

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
# 3. Transformation (dbt)
Dockerfile: Create this file "Dockerfile" in notebook and change it so it does not have a file type in Data-engineering-project/ and paste:
```bash
FROM python:3.11-slim
RUN apt-get update && apt-get install -y build-essential git curl
RUN pip install dbt-core dbt-clickhouse
WORKDIR /dbt
ENTRYPOINT ["dbt"]
```
dbt_project.yml
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

# Build and Run dbt container:
Open PowerShell/Terminal in Data-engineering-project and run:
`docker build -t my-dbt-clickhouse`

```bash
docker run -it --rm `
  -v ${PWD}:/dbt `
  -v C:\Users\lamps\.dbt:/root/.dbt `
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

# Silver Layer
Model: silver_mtr_clean.sql

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
Save to: models/silver/silver_mtr_clean.sql

Schema: models/silver/schema.yml
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
Dimension – dim_company.sql (SCD Type 2)
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
FROM {{ ref('raw_company_data') }}
```
Dimension – dim_date.sql (SCD Type 0)
location: models/gold/dim_date.sql
```bash
SELECT
    toYYYYMMDD(date) AS date_id,
    date,
    formatDateTime(date, '%A') AS day_of_week,
    formatDateTime(date, '%B') AS month,
    toQuarter(date) AS quarter,
    NOT toDayOfWeek(date) IN (6, 7) AS is_weekday,
    toWeek(date) AS week_number
FROM (
    SELECT
        addDays(toDate('2025-01-01'), number) AS date
    FROM system.numbers
    LIMIT 365
)
```

Dimension – dim_activity_type.sql (SCD Type 0)
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
Schema - models/gold/schema.yml
```bash
version: 2

models:
  - name: fact_activity_event
    description: "Fact table for activity events"
    columns:
      - name: event_id
        tests:
          - unique
          - not_null
      - name: company_id
        tests:
          - not_null
      - name: activity_type_id
        tests:
          - not_null
      - name: status
        tests:
          - not_null
      - name: duration_days
        description: "Duration in days"
      - name: risk_level
        description: "Risk classification"

  - name: dim_company
    description: "Company dimension with SCD Type 2"
    columns:
      - name: company_id
        tests:
          - unique
          - not_null
      - name: valid_from
        tests:
          - not_null
      - name: valid_to
        tests:
          - not_null

  - name: dim_date
    description: "Date dimension for calendar analysis"
    columns:
      - name: date_id
        tests:
          - unique
          - not_null

  - name: dim_activity_type
    description: "Activity type dimension with risk classification"
    columns:
      - name: activity_type_id
        tests:
          - unique
          - not_null
```
Add them to models/gold/ and then run
```bash
dbt run --select gold
dbt test --select gold
```
# Orchestration – Airflow + dbt


