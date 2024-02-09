import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.types import StructType, StructField, StringType,IntegerType, FloatType, TimestampType

def main():
    appName = "testme"
    spark_session = SparkSession.builder.enableHiveSupport().appName(appName).getOrCreate()
    spark_context = SparkContext.getOrCreate()
    spark_context.setLogLevel("ERROR")
    print(spark_session)
    print(spark_context)
    rdd = spark_context.parallelize([1,2,3,4,5,6,7,8,9,10,11,12]).distinct()
    rddCollect = rdd.collect()
    print("Number of Partitions: " + str(rdd.getNumPartitions()))
    print(rddCollect)
    # Call toDF() with the explicit schema
    schema = StructType([StructField("value", IntegerType())])
    df = spark_session.createDataFrame(rdd.map(lambda x: (x,)), schema=schema)
    df.explain("extended")

if __name__ == "__main__":
  main()
