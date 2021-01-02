"""Initializer for Airflow DAGS

"""

__author__='shaurya'

import collections
import json
import logging
import os
import sys
import re
from time import perf_counter
import yaml
from airflow.models import connection
from jinja2 import Environment,FileSystemLoader
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
import codecs
from configparser import SafeConfigParser
import itertools

from yaml.loader import Loader

# Setup logging
def setup_logging(log_name,log_level):
    """Return logger
    params:
        log_name, log_level
    returns:
        logger
    """
    log_format="%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d"
    logging.basicConfig(level=log_level.upper(),
                        format=log_format,
                        datefmt="%Y-%m-%d %H:%M:%S")
    logging.StreamHandler(sys.__stdout__)
    log=logging.getLogger().setLevel(log_level.upper())
    return log

# Timer decorator
def timeit(f):
    """Returns decorator wrapper for functions to check runtime
    params:
        runtime function
    returns:
        output of wrapped function and time to run
    """
    def runtime(*args,**kwargs):
        start=perf_counter()
        result=f(*args,**kwargs)
        end=perf_counter()
        logging.info(f"Time to run: {(end-start):.2f}s")
        return result
    return runtime

# Generate config

############ test Jinja config generator ##############################
# jinja_env=Environment(loader=FileSystemLoader("../config/templates/"))
# template=jinja_env.get_or_select_template('connections.yaml')
# # load
# parser=SafeConfigParser()
# with codecs.open("../config/config.ini",'r',encoding='utf-8') as f:
#     parser.readfp(f)

# for section in parser.sections():
#     print(parser.items(section))
#     print(dict(parser.items(section)))
# test=[dict(parser.items(section)) for section in parser.sections()]
# new_dict={}
# for item in test:
#     for k,v in item.items():
#         new_dict[k]=v

# def config_dict(path_to_file):
#     parser=SafeConfigParser()
#     with codecs.open("../config/config.ini","r",encoding='utf-8') as f:
#         parser.read_file(f)
#     config={}
#     for _ in test:
#         for k,v in _.items():
#             config[k]=v
#     return config

# config=config_dict("../config/config.ini")
# print(template.render(config))
# type(template.render(config))
# #? first generate and then read with yaml?
# with codecs.open("../config/rendered_templates/connections.yaml","w",encoding='utf-8') as config_file:
#     config_file.write(template.render(config))

# #? view rendered config yaml
# with codecs.open("../config/rendered_templates/connections.yaml","r",encoding='utf-8') as config_file:
#     test=yaml.load(config_file)
# print(test['S3toPostgres'])

# # check os path differences
# os.path.abspath("../config/config.ini") # full path from home
# os.path.abspath(os.path.join('config','../config/config.ini'))
# os.path.normpath("../config/config.ini") # normalized path
# os.path.dirname(os.path.abspath(os.path.dirname('../config/config.ini')))
#########################################################################
#! config.ini -> render jinja2 yaml template -> write to yaml -> load with pyyaml
def read_parse_config(template_dir,template_file,config_file,class_name):
    """Return dictionary of rendered configuration params for a dag run
    params:
        template_dir: directory where jinja2 yaml templates are stored
        template_file: the template file to render for this dag run
        config_file: config.ini file with parameters to pass
        class_name: the class for which the dag has to run
    returns:
        dict of rendered params for the class dag run
    example:
        >>> read_parse_config("../config/templates/","connections.yaml","../config/config.ini",'S3toRedshift')
            Out:
                {
                    'aws key': ...,
                    'aws secret': ...,
                    'db_user': ...
                }        
    """
    def config_dict(config_file):
        parser=SafeConfigParser()
        with codecs.open(config_file,"r",encoding='utf-8') as f:
            parser.read_file(f)
        config={}
        for _ in [dict(parser.items(section)) for section in parser.sections()]:
            for k,v in _.items():
                config[k]=v
        return config
    jinja_env=Environment(loader=FileSystemLoader(template_dir))
    template=jinja_env.get_template(template_file)
    config=config_dict(config_file)
    # Generate jinja2 yaml template with rendered configuration
    with codecs.open(os.path.join(os.path.dirname(config_file),'rendered_templates',str(class_name+".yaml")),"w",encoding='utf-8') as f:
        f.write(template.render(config))
    # Load yaml file with rendered aliases
    with codecs.open(os.path.join(os.path.dirname(config_file),'rendered_templates',str(class_name+".yaml")),"r",encoding='utf-8') as f:
        rendered_config=yaml.load(f)
    return rendered_config[class_name]
                
print(read_parse_config('../config/templates/','connections.yaml','../config/config.ini','S3toPostgres'))

# TODO: add airflow_add_connection
# TODO: execute_sql
# TODO: render sql query templates with parameters
# TODO: add execute_sql