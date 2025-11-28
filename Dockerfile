FROM python:3.11-slim

# Paigalda vajalikud süsteemi tööriistad
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    wget \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Uuenda pip ja setuptools, et saada prebuilt wheel'id
RUN pip install --upgrade pip setuptools wheel

# Paigalda Python paketid ilma liiga rangete versioonipiiranguteta
RUN pip install --no-cache-dir \
    duckdb \
    pyarrow>=15.0.0,<16.0.0 \
    pyiceberg \
    pandas \
    clickhouse-driver \
    dbt-core \
    dbt-clickhouse

# Töökataloog DBT või Python projektide jaoks
WORKDIR /project
# Käivituspunkt – bash, et saaksid konteineris käske jooksutada
ENTRYPOINT ["bash"]

