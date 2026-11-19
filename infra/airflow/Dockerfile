FROM apache/airflow:3.1.4-python3.11

RUN python -m venv /home/airflow/retail-venv \
    && /home/airflow/retail-venv/bin/pip install --no-cache-dir dbt-postgres==1.9.1 psycopg[binary]==3.2.10
