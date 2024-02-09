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
from pyspark.sql.functions import struct
from pyspark.sql.types import *

spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()
sc = SparkContext.getOrCreate()
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10]).distinct()
Schema = StructType([ StructField("ID", IntegerType(), False)])
df = spark.createDataFrame(data=rdd,schema=Schema)
df.printSchema()
time.sleep(30) # Pause
rdd.toDebugString()
