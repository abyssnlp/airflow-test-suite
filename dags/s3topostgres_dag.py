"""Airflow DAG S3 to postgres
    @author:Shaurya
    @date: 2021-01-03
"""

from pprint import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago

# TODO: add test to the DAG before running