FROM quay.io/astronomer/astro-runtime:12.7.1
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate

RUN python -m venv snowpark_venv && source snowpark_venv/bin/activate && \
    pip install --no-cache-dir pandas numpy scikit-learn joblib \
    snowflake-snowpark-python[pandas]>=1.11.1 snowflake-ml-python>=1.1.2 && deactivate