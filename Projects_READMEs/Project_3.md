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
pip install minio --user
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
### 1. Load into ClickHouse
Since we had trouble with re-running the necessary steps of project 2, we created a small sample database `db_demo.sql` for the simplicity of demonstrating data masking and roles creation.


Load the sample database into ClickHouse:

```bash
docker exec -it clickhouse clickhouse-client --multiquery --queries-file=/sql/db_demo.sql
```

### 2. Data masking

Paste the sql from `sql/db_demo.sql` to ClickHouse. This creates a view that masks the fileds of `registry_code`, `normalized_adddress` and `postal_code`.


### 3. Create roles

Let's create analyst_full role and grant it access. Run the sql from `sql/analyst_full.sql` in Clickhouse.

Now, we create analyst_limited and grant it limited access. Run the sql from `sql/analyst_limited.sql` in Clickhouse.


### 4. Example queries with both roles



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
