# Project 3

## Apache Iceberg

## Environment Setup

Start services:
```bash
docker compose up --build -d
```
Create MinIO bucket:
Login: http://localhost:9003
User: minio_user
Password: minio_pass
Bucket: warehouse

```
Install dependencies in Airflow containers:
Airflow webserver:
```bash
docker exec -it airflow-webserver bash
pip install --upgrade pyiceberg "pyarrow>=15.0.0"
exit
```
Airflow scheduler:
```bash
docker exec -it airflow-scheduler bash
pip install --upgrade pyiceberg "pyarrow>=15.0.0"
exit
```
Re-run bash and check PyIceberg:
```bash
docker exec -it airflow-webserver bash
python -c "import pyiceberg, minio; print('ok')"
exit
```
Quick import check
```bash
docker exec -it airflow-webserver bash
python -c "import pyiceberg, pyarrow; print(pyiceberg.__version__, pyarrow.__version__)"
exit
```
Ingest CSV into Iceberg (bronze layer)
```bash
docker exec -it airflow-webserver bash
python /opt/airflow/repo/Implementation/iceberg/bronze_mtr_iceberg_ingest.py
```
You should see:

    ✅ DuckDB table created

    ✅ Namespace bronze ensured

    ✅ Table bronze.mtr_iceberg created

    ✅ Data appended
<img width="1312" height="136" alt="image" src="https://github.com/user-attachments/assets/928a6781-526c-4d0a-88de-befb736a1100" />

Verify: PyIceberg and ClickHouse
Verify with PyIceberg
```bash
docker exec -it airflow-webserver bash
python -c "
from pyiceberg.catalog import load_catalog
catalog = load_catalog('rest')
table = catalog.load_table('bronze.mtr_iceberg')
print(table.schema())
print(table.scan().to_arrow().to_pandas().head())
"
exit
```
<img width="1271" height="503" alt="image" src="https://github.com/user-attachments/assets/f63ef6a1-20ce-42df-bc35-403690864250" />

## ClickHouse
### Load into ClickHouse

...

### Create roles

Let's create analyst_full role and grant it access.
```bash

-- create role
CREATE ROLE IF NOT EXISTS analyst_full;

-- create user 
CREATE USER IF NOT EXISTS full_user
IDENTIFIED BY 'full_strong_password';

-- give the user this role 
GRANT analyst_full TO full_user;

-- give the user select rights only on required columns.
GRANT SELECT (name, registry_code, vat_code, initial_registration_date, normalized_address, postal_code, legal_form , legal_form_subtype)
ON raw_company_data
TO analyst_full;

GRANT SELECT (registrikood, tgevusala, alguskuupaev, loppkuupaev, staatus, allikas)
ON bronze_mtr_raw 
TO analyst_full;

```
Now, we create analyst_limited and grant it limited access.
```bash

-- create role
CREATE ROLE IF NOT EXISTS analyst_limited;

-- create user 
CREATE USER IF NOT EXISTS limited_user
IDENTIFIED BY 'limited_strong_password';

-- give the user this role 
GRANT analyst_limited TO limited_user;

-- give the user select rights only on required columns.
GRANT SELECT (name, registry_code, vat_code, initial_registration_date, legal_form , legal_form_subtype)
ON raw_company_data
TO analyst_limited;

GRANT SELECT (registrikood, tegevusala, alguskuupaev, loppkuupaev)
ON bronze_mtr_raw 
TO analyst_limited;

```

## OpenMetadata

## Environment Setup

Use docker ```compose up -d``` to start the OpenMetadata services.
Note: this can take several minutes.

Then, navigate to the OpenMetadata UI by opening your browser and going to localhost:8585

The default Username and Password are:
```bash
Username - admin@open-metadata.org
Password - admin
```
