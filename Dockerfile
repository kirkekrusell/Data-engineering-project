FROM python:3.11-slim

RUN apt-get update && apt-get install -y build-essential git curl

RUN pip install dbt-core dbt-clickhouse

WORKDIR /dbt

ENTRYPOINT ["dbt"]
