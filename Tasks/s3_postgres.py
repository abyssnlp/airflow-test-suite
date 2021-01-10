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


class S3toPostgres(BaseETL):
    def __init__(self,bucket_name,table_name, **kwargs) -> None:
        super(S3toPostgres,self).__init__()
        self.config=read_parse_config(str(self.__class__.__name__),**kwargs)       

    def extract(self,**kwargs):
        s3=boto3.resource('s3',aws_access_key=self.config['aws_key'],aws_secret_access_key=config['aws_secret'])
        bucket=s3.Bucket(self.bucket_name)
        for obj in bucket.objects.all():
            data=obj.get()['Body'].read()
        return data
    
    def transform(self,data,**kwargs):
        json_list=[x for x in data.decode('utf-8').split('\n')]
        df_json=pd.DataFrame()
        for line in json_list:
            try:
                df_line=pd.DataFrame(json.loads(line).items()).T
                df_line.columns=list(df_line.iloc[0])
                df_line=df_line.drop(0)
                df_json=pd.concat([df_line,df_json])
            except json.JSONDecodeError as e:
                continue
        return df_json.reset_index(drop=True)

    def load(self,dataframe,**kwargs):
        pg_connstring=self.config['db_postgres']
        engine=create_engine(pg_connstring)
        dataframe.to_sql('marketing_data',engine=engine,if_exists='append')