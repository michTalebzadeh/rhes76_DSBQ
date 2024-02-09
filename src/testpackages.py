from __future__ import print_function
import sys
import os
import pkgutil
import pkg_resources
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark import SparkFiles
#from src import config
import config
from config import oracle_url
def main():
    spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")
    print("\n printing sys.path")
    for p in sys.path:
       print(p)
    user_paths = os.environ['PYTHONPATH'].split(os.pathsep)
    print("\n Printing user_paths")
    for p in user_paths:
       print(p)
    v = sys.version
    print("\n python version")
    print(v)
    d = os.getcwd()
    print("\n current working directory", d)
    ### Run this in local mode
    ### Read yaml file and printout config and then cut and past in method def loadDict() in configure.py
    import yaml
    with open("/home/hduser/dba/bin/python/DSBQ/conf/config.yml", 'r') as file:
       config = yaml.safe_load(file)
    print(config)
    appName = config['common']['appName']
    print(appName)
    print(oracle_url)
    import yaml
    import cx_Oracle
    #print(cx_Oracle.clientversion())
    #import tensorflow
    s = sys.modules
    for m in sys.modules:
      print(m)
    # read config.yml from /opt/spark/workfiles
    with open("////home/hduser/dba/bin/python/DSBQ/conf/config.yml", 'r') as file:
      config2 = yaml.safe_load(file)
      appName2 = config2['common']['appName']
      print(appName2)
    sc.stop()

if __name__ == "__main__":
  main()
