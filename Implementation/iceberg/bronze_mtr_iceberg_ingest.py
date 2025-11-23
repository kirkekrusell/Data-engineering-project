import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError, NamespaceAlreadyExistsError

# ------------------------------
# DuckDB Connection (persistent DB file)
# ------------------------------
conn = duckdb.connect("project.duckdb")  # persistent DB file
conn.install_extension("httpfs")
conn.load_extension("httpfs")

# Configure S3 (MinIO)
conn.sql("""
SET s3_region='us-east-1';
SET s3_url_style='path';
SET s3_endpoint='minio:9000';
SET s3_access_key_id='minio_user';
SET s3_secret_access_key='minio_pass';
SET s3_use_ssl=false;
""")

# ------------------------------
# Load CSV into DuckDB as raw_mtr_data
# ------------------------------
csv_path = "/opt/airflow/repo/Implementation/data/mtr_test_2_utf8.csv"
conn.sql(f"""
CREATE OR REPLACE TABLE raw_mtr_data AS
SELECT * FROM read_csv_auto('{csv_path}', delim=';')
""")

print("✅ DuckDB table 'raw_mtr_data' created from CSV")

# ------------------------------
# Load PyIceberg REST Catalog
# ------------------------------
catalog = load_catalog(name="rest")
namespace = "bronze"
table_name = "mtr_iceberg"

# ------------------------------
# Create namespace if it doesn't exist
# ------------------------------
try:
    catalog.create_namespace(namespace)
    print(f"✅ Namespace '{namespace}' created")
except NamespaceAlreadyExistsError:
    print(f"⚠ Namespace '{namespace}' already exists, skipping creation")

# ------------------------------
# Select only bronze columns
# ------------------------------

arrow_reader = conn.sql("""
SELECT
    Registrikood AS registrikood,
    Tegevusala AS tegevusala,
    "Kehtivuse algus" AS alguskuupaev,
    "Kehtivuse lõpp" AS loppkuupaev,
    Kehtiv AS staatus,
    Lisainfo AS allikas
FROM raw_mtr_data
""").arrow()

arrow_table = pa.Table.from_batches(arrow_reader)

# ------------------------------
# Drop table if exists
# ------------------------------
try:
    catalog.drop_table(f"{namespace}.{table_name}")
    print(f"⚠ Table '{namespace}.{table_name}' existed and was dropped")
except NoSuchTableError:
    print(f"⚠ Table '{namespace}.{table_name}' did not exist, skipping drop")

# ------------------------------
# Create Iceberg table
# ------------------------------
table = catalog.create_table(
    identifier=f"{namespace}.{table_name}",
    schema=arrow_table.schema,
)
print(f"✅ Table '{namespace}.{table_name}' created")

# ------------------------------
# Append data and commit
# ------------------------------
append = table.new_append()       # loo uus append transaction
append.add(arrow_table)           # lisa Arrow tabel
append.commit()                   # commit -> kirjutab metadata ja data failid MinIO-sse
print(f"✅ Data committed to '{namespace}.{table_name}'")
