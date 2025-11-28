# ---- Optimeeritud Python 3.10 baaspilt ----
FROM python:3.10-slim

# ---- Paigalda build tools ja vajalikud system libraries ----
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

# ---- Set working directory ----
WORKDIR /dbt

# ---- Install Python dependencies ----
# Fikseeritud versioonid, mis on stabiilsed ja ei tekita PyArrow build probleeme
RUN pip install --no-cache-dir \
    dbt-core==1.6.2 \
    dbt-clickhouse==1.6.0 \
    duckdb==1.9.0 \
    pyarrow==12.0.0 \
    pyiceberg==0.10.0 \
    pandas==2.1.1 \
    clickhouse-driver==0.2.3

# ---- Entrypoint DBT jaoks ----
ENTRYPOINT ["dbt"]


