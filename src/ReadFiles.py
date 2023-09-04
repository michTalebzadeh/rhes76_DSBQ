from __future__ import print_function
from config import config
import sys
#print (sys.path)
from sparkutils import sparkstuff as s	
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType,IntegerType, FloatType, TimestampType
from google.cloud import bigquery
import os
import locale
locale.setlocale(locale.LC_ALL, 'en_GB')
from pyspark.sql import functions as F
import pyspark.sql.functions as F
from decimal import getcontext, Decimal
import datetime

def sendToSink(df, batchId):
    if(len(df.take(1))) > 0:
        print(f"""batchId is {batchId}""")
        df. persist()
        #df.select(col('rowkey')).show(10000,False)
        rows = df.count()
        print(f""" Total records processed in this run = {rows}""")
        # write to BigQuery batch table
        s.writeTableToBQ(df, "append", config['MDVariables']['targetDataset'],config['MDVariables']['targetTable'])
        df.unpersist()
        print(f"""wrote to DB""")

    else:
        print("DataFrame is empty")

def sendToControl(dfnewtopic, batchId):
    if(len(dfnewtopic.take(1))) > 0:
        print(f"""newtopic batchId is {batchId}""")
        dfnewtopic.show(100,False)
        queue = dfnewtopic.select(col("queue")).collect()[0][0]
        status = dfnewtopic.select(col("status")).collect()[0][0]
 
        if((queue == config['MDVariables']['topic']) & (status == 'false')):
          spark_session = s.spark_session(config['common']['appName'])
          active = spark_session.streams.active
          for e in active:
             #print(e)
             name = e.name
             if(name == config['MDVariables']['topic']):
                print(f"""Terminating streaming process {name}""")
                e.stop()
    else:
        print("DataFrame newtopic is empty")


class MDStreaming:
    def __init__(self,spark_session,spark_context):
        self.spark = spark_session
        self.sc = spark_context
        self.config = config

    def fetch_data(self):
        self.sc.setLogLevel("ERROR")
        #{"rowkey":"c9289c6e-77f5-4a65-9dfb-d6b675d67cff","ticker":"MSFT", "timeissued":"2021-02-23T08:42:23", "price":31.12}
        schema = StructType().add("rowkey", StringType()).add("ticker", StringType()).add("timeissued", TimestampType()).add("price", FloatType())
        # Get todays's date
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        #hdfs_path = "hdfs://rhes75:9000/data/prices/"+today
        """
        Create an input stream that monitors a Hadoop-compatible filesystem for new files and reads them as text files (using key as LongWritable, value as Text and input format as TextInputFormat). 
        Files must be written to the monitored directory by "moving" them from another location within the same file system. File names starting with . are ignored.
        So, the method expects the path to a directory in the parameter.
        ssc.textFileStream("file:///Users/userName/Documents/Notes/MoreNotes/")
        """
        data_path = "file:///mnt/gs/prices/data/"
        checkpoint_path = "file:///mnt/gs/prices/chkpt/"
        #data_path = "file:///ssd/hduser/prices/data"
        #checkpoint_path = "file:///ssd/hduser/prices/chkpt"
        try:
            streamingDataFrame = self.spark \
                .readStream \
                .option('newFilesOnly', 'true') \
                .option('header', 'true') \
                .option('maxFilesPerTrigger', 1000) \
                .option('latestFirst', 'false') \
                .text(data_path) \
                .select(from_json(col("value").cast("string"), schema).alias("parsed_value"))


            streamingDataFrame.printSchema()
            result = streamingDataFrame.select( \
                     col("parsed_value.rowkey").alias("rowkey") \
                   , col("parsed_value.ticker").alias("ticker") \
                   , col("parsed_value.timeissued").alias("timeissued") \
                   , col("parsed_value.price").alias("price")). \
                     writeStream. \
                     outputMode('append'). \
                     option("truncate", "false"). \
                     foreachBatch(sendToSink). \
                     queryName('trailFiles'). \
                     trigger(once = True). \
                     option('checkpointLocation', checkpoint_path). \
                     start(data_path)

        except Exception as e:
                print(f"""{e}, quitting""")
                sys.exit(1)
            
        result.awaitTermination()
        
if __name__ == "__main__":
    
    #appName = config['common']['appName']
    appName = "trailFiles"
    spark_session = s.spark_session(appName)
    spark_session = s.setSparkConfStreaming(spark_session)
    spark_session = s.setSparkConfBQ(spark_session)
    spark_context = s.sparkcontext()
    mdstreaming = MDStreaming(spark_session, spark_context)
    streamingDataFrame = mdstreaming.fetch_data()
