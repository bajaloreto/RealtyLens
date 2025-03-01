FROM quay.io/astronomer/astro-runtime:12.7.1

# Install airflow packages without dbt
COPY requirements-airflow.txt .
RUN pip install --no-cache-dir -r requirements-airflow.txt

# Create separate DBT environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install dbt-snowflake==1.6.0 && deactivate