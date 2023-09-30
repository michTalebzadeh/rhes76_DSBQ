import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType,IntegerType, FloatType, TimestampType
import time
def main():
    appName = "skew"
    spark = SparkSession.builder.appName(appName).getOrCreate()
    spark_context = SparkContext.getOrCreate()
    spark_context.setLogLevel("ERROR")
    df_uniform = spark.createDataFrame([i for i in range0(100)], IntegerType())
    df_uniform = df_uniform.withColumn("partitionId", spark_partition_id())
    print("Number of Partitions: "+str(df_uniform.rdd.getNumPartitions()))
    df_uniform.groupby([df_uniform.partitionId]).count().sort(df_uniform.partitionId).show(5)
    df_uniform.alias("left").join(df_uniform.alias("right"),"value", "inner").count()

    ##print(f"""spark.default.parallelism is {spark.conf.get("spark.default.parallelism")}""")
    print(f"""Spark.sql.shuffle.partitions is {spark.conf.get("spark.sql.shuffle.partitions")}""")
    df0 = spark.createDataFrame([0] * 9999998, IntegerType()).repartition(1)
    df1 = spark.createDataFrame([1], IntegerType()).repartition(1)
    df2 = spark.createDataFrame([2], IntegerType()).repartition(1)
    df_skew = df0.union(df1).union(df2)
    df_skew = df_skew.withColumn("partitionId", spark_partition_id())
    ## If we apply the same function call again, we get what we want to see for the one partition with much more data than the other two.
    df_skew.groupby([df_skew.partitionId]).count().sort(df_skew.partitionId).show(5)
    ## simulate reading to first round robin distribute the key
    #df_skew = df_skew.repartition(3)
    df_skew.join(df_uniform.select("value"),"value", "inner").count()
    # salt range is from 1 to spark.conf.get("spark.sql.shuffle.partitions")
    df_left = df_skew.withColumn("salt", (rand() * spark.conf.get("spark.sql.shuffle.partitions")).cast("int")).show(5)
    df_right = df_uniform.withColumn("salt_temp", array([lit(i) for i in range(int(spark.conf.get("spark.sql.shuffle.partitions")))])).show(5)
    time.sleep(60) # Pause 

if __name__ == "__main__":
  main()
