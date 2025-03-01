FROM quay.io/astronomer/astro-runtime:12.7.1

# Install main packages
COPY requirements-airflow.txt .
RUN pip install --no-cache-dir -r requirements-airflow.txt

# Create and run a script to install dbt and cosmos with a fixed sqlparse version
RUN echo '#!/bin/bash \n\
python -m venv dbt_venv \n\
source dbt_venv/bin/activate \n\
pip install --no-cache-dir sqlparse==0.4.4 \n\
pip install --no-cache-dir dbt-snowflake==1.6.0 astronomer-cosmos==1.9.0 \n\
deactivate' > /tmp/setup_dbt.sh && \
chmod +x /tmp/setup_dbt.sh && \
/tmp/setup_dbt.sh

# Add dbt to path and make packages accessible
ENV PATH="/usr/local/airflow/dbt_venv/bin:${PATH}"
ENV PYTHONPATH="${PYTHONPATH}:/usr/local/airflow/dbt_venv/lib/python3.12/site-packages"