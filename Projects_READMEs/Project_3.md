# Project 3

## Updates to project 2
### Updated SQL queries

1. How many companies have multiple activity notices and operate in multiple sectors?
```sql
SELECT COUNT(*) AS company_count
FROM (
    SELECT dim_companies_id
    FROM fact_mtr
    GROUP BY dim_companies_id
    HAVING COUNT(DISTINCT dim_activity_id) > 1
       AND COUNT(DISTINCT mtr_registry_code) > 1
) AS sub;
```

2. How many companies registered their economic activity areas in the same year they were established?
```sql
SELECT COUNT(DISTINCT fae.company_id) AS same_year_company_count
FROM fact_activity_event fae
JOIN dim_company dc ON fae.company_id = dc.company_id
WHERE EXTRACT(YEAR FROM fae.start_date) = EXTRACT(YEAR FROM dc.initial_registration_date);
```

3. How many companies have terminated at least one economic activity notice?
```sql
SELECT COUNT(DISTINCT company_id) AS terminated_company_count
FROM fact_activity_event
WHERE end_date < CURRENT_DATE;
```

4. What is the average duration of an activity notice before it expires?
```sql
SELECT AVG(duration_days) AS avg_notice_duration
FROM fact_activity_event
WHERE end_date IS NOT NULL;
```

5. Percentage of companies with all activity notices expired?
```sql
SELECT 
    ROUND(
        100.0 * COUNT(CASE WHEN all_expired THEN 1 END) 
        / COUNT(DISTINCT company_id), 
        2
    ) AS percentage_expired_companies
FROM (
    SELECT 
        company_id,
        MAX(end_date) < CURRENT_DATE AS all_expired
    FROM fact_activity_event
    GROUP BY company_id
) AS company_status;
```

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
exit
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
**Note:** Since we had trouble with re-running the necessary steps of project 2, we created a small sample database `db_demo.sql` for the simplicity of demonstrating data masking and roles creation. The file creates and populates a table `dim_company` which is one of the tables from Gold layer.


Load the sample database into ClickHouse:

**Credentials:** 

**user**: admin

**password**: password

```bash
docker exec -it clickhouse clickhouse-client --multiquery --queries-file=/sql/db_demo.sql
```

### 2. Data masking

Paste the sql from `sql/db_demo.sql` to ClickHouse. This creates a view that masks the fileds of `registry_code`, `normalized_adddress` and `postal_code`.


### 3. Create roles

Let's create analyst_full role and grant it access. Run the sql from `sql/analyst_full.sql` in Clickhouse.

Now, we create analyst_limited and grant it limited access. Run the sql from `sql/analyst_limited.sql` in Clickhouse.


### 4. Sample queries with both roles

You can find the sample queries in `sql/sample_queries.sql`

Role credentials for ClickHouse:

| **Role**        |  **User**    | **Password**            |
|-----------------|--------------|-------------------------|
| analyst_full    | full_user    | full_strong_password    |
| analyst_limited | limited_user | limited_strong_password |



#### **Find the registry codes of companies**
**analyst_full**

<img width="626" height="181" alt="image" src="https://github.com/user-attachments/assets/3e3c02b6-b9e7-419b-ab9a-5334efd67d34" />



**analyst_limited**

<img width="622" height="186" alt="image" src="https://github.com/user-attachments/assets/07befd63-4d02-435e-bd4b-25fc78acea93" />



#### **Group companies by county**
**analyst_full**

<img width="638" height="125" alt="image" src="https://github.com/user-attachments/assets/71ebd7a5-7857-420f-ac71-c235815b78ae" />



**analyst_limited**

<img width="638" height="71" alt="image" src="https://github.com/user-attachments/assets/47ba453e-2b67-49ab-bee9-cd45b5ecfe0b" />



#### **Find companies with 5-digit postal codes starting with '44'**
**analyst_full**

<img width="638" height="68" alt="image" src="https://github.com/user-attachments/assets/108871e5-f929-40cf-a38b-3aef17ca7e54" />



**analyst_limited**

<img width="877" height="68" alt="image" src="https://github.com/user-attachments/assets/f7588a42-ab86-4d54-a174-1ee625a1f502" />



## OpenMetadata

## Environment Setup

Use docker ```compose up -d``` to start the OpenMetadata services.
Note: this can take several minutes.

Then, navigate to the OpenMetadata UI by opening your browser and going to [localhost:8585](http://localhost:8585/)

The default Username and Password are:
```bash
Username - admin@open-metadata.org
Password - admin
```

## Register Services

    In the OpenMetadata UI, go to Settings → Services → Add New Service.

### Register ClickHouse as a Database Service:

    Name: clickhouse_gold
    Host: clickhouse
    Port: 8123
    Username: admin
    Password: password
    Database: default

Save and run ingestion to discover tables/views.

### Register Superset as a Dashboard Service:

    Name: superset_prod
    Host URL: http://superset:8088
    Authentication: API key or username/password

Save and ingest dashboards.

## Add Descriptions

### Fact table: 

    fact_activity_event: “Contains activity events with company, activity type, status, and duration.”

### Dimension tables:

    dim_company: “Company attributes tracked historically using SCD Type 2.”
    dim_date: “Calendar dimension with day, month, quarter and derived flags.”
    dim_activity_type: “Activity types with risk classification (low/medium/high).”
    dim_status: “Status codes and labels for activity events.”

## Add Data Quality Tests

Navigate to a table → Data Quality → Add Test.

Add the following tests:

    Fact table foreign key check
        Table: fact_activity_event
        Column: company_id
        Test: Not Null (ensures every fact record links to a company)

    Dimension surrogate key check
        Table: dim_company
        Column: company_id
        Test: Unique (ensures each company has a distinct surrogate key)

    Additional validation
        Table: fact_activity_event
        Test: Row Count > 0 (verifies that the fact table contains data)
    
## Save tests and configure schedule (e.g., weekly on Sunday) or run manually.

### In OpenMetadata UI, go to Data Quality → Run Tests Now.

### Wait for results (Passed/Failed).

# Apache Superset

## Connect to ClickHouse

Add database connection:

    Superset UI → Data → Databases → + Database → ClickHouse.

SQLAlchemy URI (native): clickhouse+native://admin:password@clickhouse:9000/default
SQLAlchemy URI (HTTP alternative): clickhouse+http://admin:password@clickhouse:8123/default
Action: Test connection → Save.

## Register gold views as datasets:

    Superset UI → Data → Datasets → + Dataset.
    Select database: your ClickHouse connection.
    
    Choose views:
    v_companies_full
    v_companies_limited

## Build dashboard

    Create charts:

        Chart 1:
        Chart 2:
