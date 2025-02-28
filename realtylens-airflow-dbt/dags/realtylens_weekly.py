from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from include.scripts.sql_scripts_weekly import create_stages_sql, refresh_stages_sql, raw_data_load_sql
from airflow.operators.bash import BashOperator
import logging
import os 
import sys
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.constants import SourceRenderingBehavior



DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/weekly"
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


default_args = {
   'owner': 'Jonathan Musni', 
   'start_date': datetime(2025, 2, 23), 
   'retries': 0,
}

@dag(
   description='RealtyLens weekly data pipeline. This pipeline is designed to run weekly and load data from S3 into Snowflake.',
   default_args=default_args,
   schedule_interval='@weekly',
   catchup=True,
   template_searchpath='include/scripts'
)
def realtylens_weekly():
    
    ds = '{{ ds }}'

    setup_stages = SQLExecuteQueryOperator(
        task_id='setup_stages',
        sql=create_stages_sql.format(
            bucket=S3_BUCKET,
            aws_key=AWS_ACCESS_KEY_ID,
            aws_secret=AWS_SECRET_ACCESS_KEY
        ),
        conn_id='snowflake_conn',
    )
    refresh_stages = SQLExecuteQueryOperator(
        task_id='refresh_stages',
        sql=refresh_stages_sql,
        conn_id='snowflake_conn',
    )

    load_raw_data_from_s3 = SQLExecuteQueryOperator(
        task_id='load_raw_data_from_s3',
        sql=raw_data_load_sql.format(ds=ds),
        conn_id='snowflake_conn',
    )

    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={"retries": 2},
        render_config=RenderConfig(source_rendering_behavior=SourceRenderingBehavior.ALL),


    )


    # Define task dependencies
    setup_stages  >> refresh_stages >> load_raw_data_from_s3  >> transform_data

    return dag

dag = realtylens_weekly()