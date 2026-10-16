# Airflow DAGs

The monthly `monthly_retrain` DAG will live here (phase 8). It runs on a local Airflow 3
started with Docker Compose, and calls the project's own commands rather than importing
heavy libraries into the Airflow environment.
