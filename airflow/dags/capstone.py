
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import DataQualityOperator
from helpers import SqlQueries

default_args = {
    'owner': 'swiatowiec',
    'start_date': datetime(2021, 5, 5),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('data_engineering_project',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '@monthly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

immigration_to_redshift = StageToRedshiftOperator(
    task_id='Immigration_Fact_Table',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_from = 's3a://data-engineer-capstone1/',
    s3_prefix = 'immigration.parquet',
    schema_to = 'public',
    table_to = 'immigration',
    options = ["FORMAT AS PARQUET"],
    dag=dag
)


temperature_to_redshift = StageToRedshiftOperator(
    task_id='Temperature_Dimension_Table',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_from = 's3a://data-engineer-capstone1/',
    s3_prefix = 'temperature.parquet',
    schema_to = 'public',
    table_to = 'temperature',
    options = ["FORMAT AS PARQUET"],
    dag=dag
)

demographics_to_redshift = StageToRedshiftOperator(
    task_id='Demographics_Dimension_Table',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_from = 's3a://data-engineer-capstone1/',
    s3_prefix = 'demographics.parquet',
    schema_to = 'public',
    table_to = 'demographics',
    options = ["FORMAT AS PARQUET"],
    dag=dag
)

airports_to_redshift = StageToRedshiftOperator(
    task_id='Airports_Dimension_Table',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_from = 's3a://data-engineer-capstone1/',
    s3_prefix = 'airports.parquet',
    schema_to = 'public',
    table_to = 'airports',
    options = ["FORMAT AS PARQUET"],
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Data_Quality_Checks',
    redshift_conn_id = "redshift",
    tables=['immigration', 'temperature', 'demographics', 'airports'],
    dag=dag
)

end_operator = DummyOperator(task_id='End',  dag=dag)

start_operator >> immigration_to_redshift >> [temperature_to_redshift, demographics_to_redshift, airports_to_redshift] >> run_quality_checks >> end_operator
