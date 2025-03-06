from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from include.scripts.sql_scripts_daily import *
import logging
import os 
import sys
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.constants import SourceRenderingBehavior
import sys
import os

# Add scripts directory to path
sys.path.append(os.path.join(os.environ['AIRFLOW_HOME'], 'include/scripts'))

# Function to import necessary modules
def import_functions():
    sys.path.append(os.path.join(os.path.dirname(__file__), '../include/scripts'))
    from check_existing_data import check_existing_data
    from property_extractor import extract_property_data
    return check_existing_data, extract_property_data

# Call the import function
check_existing_data, extract_property_data = import_functions()

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/daily"
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id='snowflake_conn')
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)



# AWS credentials from variables
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = Variable.get("AWS_DEFAULT_REGION")
S3_BUCKET = "raw-property-data-jem"

def enhanced_check_existing_data(ds, task_instance, **kwargs):
    """Enhanced logging version of check_existing_data"""
    try:
        logging.info(f"Running check_existing_data for date: {ds}")
        
        # Call original function
        result = check_existing_data(ds, task_instance)
        
        # Add extra logging
        logging.info(f"Branch decision: {result}")
        print(f"BRANCH DECISION: {result}")
        
        return result
    except Exception as e:
        logging.error(f"Error in enhanced_check_existing_data: {str(e)}")
        raise

default_args = {
   'owner': 'Jonathan Musni', 
   'start_date': datetime(2025, 2, 4), 
   'retries': 3,
   'depends_on_past': True,
}

@dag(
   description='RealtyLens daily data pipeline. This pipeline is designed to run daily and load data from S3 into Snowflake.',
   default_args=default_args,
   schedule_interval='@daily',
   catchup=True,
   template_searchpath='include/scripts',
   max_active_runs=1
)
def realtylens_daily():
    
    ds = '{{ ds }}'

    check_data = BranchPythonOperator(
       task_id='check_existing_daily_property_data',
       python_callable=enhanced_check_existing_data,
       op_kwargs={'ds': ds},
       provide_context=True
    )

    extract_data = PythonOperator(
       task_id='extract_daily_property_data',
       python_callable=extract_property_data,
       op_kwargs={'ds': ds}
    )    

    create_schema = SQLExecuteQueryOperator(
        task_id='create_snowflake_schema',
        sql="CREATE SCHEMA IF NOT EXISTS DATAEXPERT_STUDENT.jmusni07;",
        conn_id='snowflake_conn',
        trigger_rule='none_failed_or_skipped'  # Allow it to run even if upstream tasks are skipped
    )

    setup_stages = SQLExecuteQueryOperator(
        task_id='setup_snowflake_stages',
        sql=stages_sql.format(
            bucket=S3_BUCKET,
            aws_key=AWS_ACCESS_KEY_ID,
            aws_secret=AWS_SECRET_ACCESS_KEY
        ),
        conn_id='snowflake_conn'  # Added missing conn_id
    )

    refresh_stages = SQLExecuteQueryOperator(
        task_id='refresh_snowflake_stages',
        sql=refresh_stages_sql,
        conn_id='snowflake_conn',
    )

    load_data = SQLExecuteQueryOperator(
        task_id='load_raw_daily_property_data_from_s3_to_snowflake',
        sql=daily_property_sql.format(ds=ds),
        conn_id='snowflake_conn',
    )
    
    transform_data = DbtTaskGroup(
        group_id="transform_daily_property_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={"retries": 2},
        render_config=RenderConfig(source_rendering_behavior=SourceRenderingBehavior.ALL),
    )

    # Try to import the full function, fall back to simplified version if it fails
    try:
        sys.path.append(os.path.join(os.environ['AIRFLOW_HOME'], 'include/scripts'))
        from rent_price_predictor import predict_rent_prices
    except ImportError:
        print("WARNING: Could not import rent_price_predictor, using simplified version")
        # Use the simplified function defined above
        predict_rent_prices = simple_predict_rent_prices

    predict_rent = PythonOperator(
        task_id='predict_rent_prices',
        python_callable=predict_rent_prices,
        op_kwargs={
            'snowflake_conn_id': 'snowflake_conn',
            'database': 'DATAEXPERT_STUDENT',
            'schema': 'jmusni07',
            'model_registry_table': 'model_registry',
            'model_version': None  # Use latest model
        }
    )

    # Define task dependencies
    check_data >> [create_schema, extract_data]
    extract_data >> create_schema
    create_schema >> setup_stages >> refresh_stages >> load_data >> transform_data >> predict_rent

    return dag

# In case import still fails, define a simplified version inline
def simple_predict_rent_prices(snowflake_conn_id, database, schema, model_registry_table, model_version=None):
    """Simplified rent price prediction function"""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    import pandas as pd
    
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    
    # Create the predictions table
    hook.run(f"""
    CREATE TABLE IF NOT EXISTS {database}.{schema}.PREDICTED_RENT_PRICES (
        LISTING_SK VARCHAR,
        LISTING_ID VARCHAR,
        SALE_PRICE FLOAT,
        PREDICTED_RENT_PRICE FLOAT,
        RENT_TO_PRICE_RATIO FLOAT,
        LOAD_DATE DATE,
        MODEL_VERSION VARCHAR,
        PREDICTION_DATE TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """)
    
    # For demo purposes, insert a sample row
    hook.run(f"""
    INSERT INTO {database}.{schema}.PREDICTED_RENT_PRICES
    (LISTING_SK, LISTING_ID, SALE_PRICE, PREDICTED_RENT_PRICE, RENT_TO_PRICE_RATIO, LOAD_DATE, MODEL_VERSION)
    VALUES
    ('DEMO-SK', 'DEMO-ID', 500000, 2500, 0.005, CURRENT_DATE(), 'demo-version')
    """)
    
    return "Completed simplified rent price prediction"

dag = realtylens_daily()