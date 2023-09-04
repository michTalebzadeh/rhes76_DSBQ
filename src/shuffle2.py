from __future__ import print_function
import sys
import os
import pkgutil
import pkg_resources
import pyspark
import time
from pyspark import SparkConf, SparkContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()
sc = SparkContext.getOrCreate()
df = sc.parallelize(range(100)).map(lambda x: (x, )).toDF(["x"])
df.orderBy(rand()).show(3)
time.sleep(30) # Pause
