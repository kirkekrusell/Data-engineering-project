FROM python:3.11-slim

# Paigalda build-tools (gcc/g++, make) ja muud vajalikud utiliidid
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Paigalda dbt + ClickHouse adapter
RUN pip install --no-cache-dir dbt-core dbt-clickhouse

# Paigalda DuckDB, PyArrow, PyIceberg, Pandas, ClickHouse driver
# Fikseeritud versioonid tagavad stabiilsuse
RUN pip install --no-cache-dir \
    duckdb \
    "pyarrow==15.0.2" \
    "pyiceberg==0.10.0" \
    pandas \
    clickhouse-driver

WORKDIR /dbt

ENTRYPOINT ["dbt"]


