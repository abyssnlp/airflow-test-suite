"""AWS S3 bucket to postgres RDS
    @author: Shaurya
    @date: 2020-01-06
"""
import os
import sys,inspect
current_dir=os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir=os.path.dirname(current_dir)
sys.path.insert(0,parent_dir)
from base.utils import read_parse_config
import boto3
import json
import pandas as pd
import itertools
from collections import Counter
from base.base_etl import BaseETL
from sqlalchemy import create_engine,MetaData,Table,Column,String,Integer,String
from datetime import datetime

class S3toPostgres(BaseETL):
    def __init__(self,bucket_name,table_name, **kwargs) -> None:
        super(S3toPostgres,self).__init__()
        self.bucket_name=bucket_name
        self.table_name=table_name
        self.config=read_parse_config(template_dir=kwargs['template_dir'],template_file=kwargs['template_file'],config_file=kwargs['config_file'],class_name=str(self.__class__.__name__))       
        self.kwargs=kwargs

    def extract(self):
        s3=boto3.resource('s3',aws_access_key_id=self.config['aws_key'],aws_secret_access_key=self.config['aws_secret'])
        bucket=s3.Bucket(self.bucket_name)
        for obj in bucket.objects.all():
            self.data=obj.get()['Body'].read()
        return self.data
    
    def transform(self):
        json_list=[x for x in self.data.decode('utf-8').split('\n')]
        self.df_json=pd.DataFrame()
        for line in json_list:
            try:
                df_line=pd.DataFrame(json.loads(line).items()).T
                df_line.columns=list(df_line.iloc[0])
                df_line=df_line.drop(0)
                self.df_json=pd.concat([df_line,self.df_json])
            except json.JSONDecodeError as e:
                continue
        return self.df_json.reset_index(drop=True)

    def load(self):
        pg_connstring=self.config['db_postgres']
        engine=create_engine(pg_connstring)
        self.df_json.to_sql('marketing_data',con=engine,if_exists='append',index=False)

if __name__ == "__main__":
    params={
        'template_dir':'/home/abyssnlp/projects/airflow-test-suite/config/templates/',
        'template_file':'connections.yaml',
        'config_file':'/home/abyssnlp/projects/airflow-test-suite/config/config.ini',
        'log_level':'info',
        'date':datetime(2021,1,10)
    }
    config=read_parse_config(params['template_dir'],params['template_file'],params['config_file'],'S3toPostgres')
    config.update(params)
    etl=S3toPostgres(**config)
    etl.run_etl()
    etl.log.info('DONE')


#### Test implementation

# params={
#     'template_dir':'../config/templates/',
#     'template_file':'connections.yaml',
#     'config_file':'../config/config.ini',
#     'log_level':'info',
#     'date':datetime(2021,1,10)
# }
# # config=read_parse_config(params['template_dir'],params['template_file'],params['config_file'],'S3toPostgres')
# config.update(params)
# etl=S3toPostgres(bucket_name='airflow-s3-bucket-test',table_name='marketing_data',**params)
# etl.run_etl()
# etl.log.info('DONE')