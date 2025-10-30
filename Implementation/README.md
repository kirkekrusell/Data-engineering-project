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


# DAG - MTR file quality check

The DAG (`validate.py`) is located in the `implementation/` folder. When setting up Airflow, you need to copy this file into the Airflow DAGs directory:

`cp implementation/validate.py airflow/dags/`

First time it does not automatically detect the dag, you need to run airflow init.

`airflow db init`


## DAG overview

This DAG reads the modified MTR file (`mtr_test_2.csv`), checks for NAs in the "Registrikood" column, removes the found rows with NAs and creates a new file version. The dag runs once a week at midnight on Sunday morning.
