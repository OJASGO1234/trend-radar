# Official Airflow image + dbt in its OWN virtualenv.
# Why a separate venv: Airflow pins hundreds of packages; installing dbt next to them
# can break either one. The DAG calls /home/airflow/dbt-venv/bin/dbt directly.
FROM apache/airflow:2.10.5-python3.12

COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt \
 && mkdir -p /opt/airflow/state
