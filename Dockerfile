FROM python:3.11-slim

# Paigalda build-tools (gcc/g++, make) ja muud vajalikud utiliidid
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Paigalda dbt + ClickHouse adapter
RUN pip install --no-cache-dir dbt-core dbt-clickhouse

# Paigalda DuckDB, PyArrow, PyIceberg, Pandas, ClickHouse driver
RUN pip install --no-cache-dir \
    duckdb \
    pyarrow \
    pyiceberg \
    pandas \
    clickhouse-driver

WORKDIR /dbt

ENTRYPOINT ["dbt"]

