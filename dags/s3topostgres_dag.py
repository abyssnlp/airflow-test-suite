"""Airflow DAG S3 to postgres
    @author:Shaurya
    @date: 2021-01-03
"""
import os,sys,inspect
current_dir=os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir=os.path.dirname(current_dir)
sys.path.insert(0,parent_dir)

from pprint import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from Tasks.s3_postgres import S3toPostgres



default_args={
    'owner':'shaurya',
    'provide_context':True,
    'depends_on_past':False,
    'wait_for_downstream':False,
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':0,
    'params':{'template_dir':'/home/abyssnlp/projects/airflow-test-suite/config/templates/',
                'template_file':'connections.yaml',
                'config_file':'/home/abyssnlp/projects/airflow-test-suite/config/config.ini',
                'log_level':'info',
                'date':datetime(2021,1,10),
                'bucket_name':'airflow-s3-bucket-test',
                'table_name':'marketing_data'}
}


# s3 to postgres dag
lake_to_db=DAG(
    dag_id='lake_to_db',
    default_args=default_args,
    schedule_interval=None, # manual or conditional trigger
    start_date=datetime(2021,1,10),
    tags=['s3','postgres','marketing']
)

# start
begin=DummyOperator(
    task_id='start_dag',
    dag=lake_to_db
)

# create table to ingest the data into
make_table=PostgresOperator(
    task_id='create_marketing_data',
    postgres_conn_id='MarketingDataPostgres',
    sql='sql/marketing_attribution_table.sql'
)


# Actual python callable
etl_data=PythonOperator(
    task_id='s3_to_postgres',
    python_callable=S3toPostgres.airflow_runner,
    dag=lake_to_db
)

# end
end=DummyOperator(
    task_id='end_dag',
    dag=lake_to_db
)

begin >> make_table >> etl_data >> end