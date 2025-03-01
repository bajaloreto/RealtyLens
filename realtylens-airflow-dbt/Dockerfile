FROM quay.io/astronomer/astro-runtime:12.7.1

# Install Airflow packages in the main environment
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create separate DBT environment if needed
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate