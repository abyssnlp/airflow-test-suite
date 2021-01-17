"""Base class for ETL runs

"""

__author__='shaurya'

import os
import logging
from datetime import datetime
from base.utils import setup_logging,timeit,read_parse_config
import subprocess
import pprint

class BaseETL:
    def __init__(self,name=None,log_level='DEBUG',data_dir=None,**kwargs) -> None:
        super().__init__()
        self.class_name=name if name is not None else self.__class__.__name__
        self.log=setup_logging(log_name=name,log_level=log_level)
        self.data_dir=data_dir
        self.parameters=kwargs
        # self.date=self.parameters['date']
        self.dag_id=kwargs.get("dag_id",self.class_name)
    
    # Virtual functions to override by  inherited classes
    def setup(self):
        pass
    
    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass

    def cleanup(self):
        pass
    
    @timeit
    def run_etl(self):
        self.setup()
        self.extract()
        self.transform()
        self.load()
        self.cleanup()

    # Airflow runner
    @classmethod
    def airflow_runner(cls,**kwargs):
        _now=datetime.now()
        #? similar to params passed to dag or callable(check?)
        params=kwargs['params']

        # DAG_id
        dag_id='voicemod_etl'
        if 'dag' in kwargs:
            dag_id=kwargs.get('dag').dag_id
        params['dag_id']=dag_id
        # DAG_name
        if 'name' not in params:
            params['name']=cls.__name__
        # DAG_config
        config=read_parse_config(params['template_dir'],params['template_file'],params['config_file'],params['name'])
        config.update(params)
        params=config
        message='\n\n\t Executing T: '+_now.isoformat()
        message+=pprint.pformat(kwargs,indent=4)
        message+='\n\n'

        log=logging.getLogger(name=params['name'])
        log.info(message)
        etl=cls(**params)
        etl.run_etl()       
        
################### Test Area ###########################
# from datetime import datetime
# test=datetime.now()
# test.isoformat()
# from pprint import pprint
# import pprint
# test_dict={
#     'a':1,
#     'b':2
# }
# pprint(test_dict,indent=10)
# pprint.pformat(test_dict,indent=4)


#########################################################

