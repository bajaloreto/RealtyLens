FROM quay.io/astronomer/astro-runtime:12.7.1

# Install airflow providers
COPY requirements-airflow.txt .
RUN pip install --no-cache-dir -r requirements-airflow.txt

# Create DBT environment with cosmos
RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake==1.6.0 astronomer-cosmos==1.9.0 && \
    deactivate

# Make accessible to Airflow
ENV PYTHONPATH="${PYTHONPATH}:/usr/local/airflow/dbt_venv/lib/python3.12/site-packages"