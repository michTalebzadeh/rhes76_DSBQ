import pytest
from pyspark.sql import SparkSession
from pyspark import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
import pyspark
from pyspark.sql import SparkSession
import pytest
import shutil
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master('local[1]') \
        .appName('test') \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

