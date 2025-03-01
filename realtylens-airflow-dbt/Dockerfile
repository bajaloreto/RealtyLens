FROM quay.io/astronomer/astro-runtime:12.7.1

# Copy constraint files
COPY constraints.txt .
COPY requirements.txt .

# Install with constraints
RUN pip install --no-cache-dir -c constraints.txt -r requirements.txt

# DBT environment setup
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate