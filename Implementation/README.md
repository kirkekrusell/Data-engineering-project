# 1. Initialize the Environment

```shell
docker-compose up -d
```

### Credentials

| Component  | Username           | Password     | Port  |
|------------|--------------------|--------------|-------|
| airflow-db | airflow            | airflow      | 5432  |
| prices-db  | prices_user        | prices_pass  | 5433  |
| pgAdmin    | admin@example.com  | admin        | 5050  |

Access pgAdmin at:
http://localhost:5050

Access Airflow at:
http://localhost:8080

to run the py file, make sure you have the airflow library installed
```shell
pip install "apache-airflow[celery]==2.8.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"
```
