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

def f(a):
    stop

def temperatures(df, batchId):
    if(len(df.take(1))) > 0:
        df.show(100,False)
        df. persist()
        AvgTemp = df.select(round(F.avg(col("temperature")))).collect()[0][0]
        df.unpersist()
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        print(f"""Average temperature at {now} from batchId {batchId} is {AvgTemp} degrees""")
    else:
        print("DataFrame s empty")

class temperatureStreaming:
    def __init__(self,spark_session,spark_context):
        self.spark = spark_session
        self.sc = spark_context
        self.config = config

    def fetch_data(self):
        self.sc.setLogLevel("ERROR")
        schema = StructType().add("rowkey", StringType()).add("timestamp", TimestampType()).add("temperature", IntegerType())
        checkpoint_path = "file:///ssd/hduser/temperature2/chkpt"
        try:

            # construct a streaming dataframe streamingDataFrame that subscribes to topic temperature
            streamingDataFrame = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", config['MDVariables']['bootstrapServers'],) \
                .option("schema.registry.url", config['MDVariables']['schemaRegistryURL']) \
                .option("group.id", config['common']['appName']) \
                .option("zookeeper.connection.timeout.ms", config['MDVariables']['zookeeperConnectionTimeoutMs']) \
                .option("rebalance.backoff.ms", config['MDVariables']['rebalanceBackoffMS']) \
                .option("zookeeper.session.timeout.ms", config['MDVariables']['zookeeperSessionTimeOutMs']) \
                .option("auto.commit.interval.ms", config['MDVariables']['autoCommitIntervalMS']) \
                .option("subscribe", "temperature") \
                .option("failOnDataLoss", "false") \
                .option("includeHeaders", "true") \
                .option("startingOffsets", "latest") \
                .load() \
                .select(from_json(col("value").cast("string"), schema).alias("parsed_value"))


            resultM = streamingDataFrame.select( \
                     col("parsed_value.rowkey").alias("rowkey") \
                   , col("parsed_value.timestamp").alias("timestamp") \
                   , col("parsed_value.temperature").alias("temperature"))
            result = resultM. \
                     withWatermark("timestamp", "5 minutes"). \
                     groupBy(window(resultM.timestamp, "5 minutes", "5 minutes")). \
                     avg('temperature'). \
                     writeStream. \
                     outputMode('complete'). \
                     option("numRows", 1000). \
                     option("truncate", "false"). \
                     format('console'). \
                     option('checkpointLocation', checkpoint_path). \
                     queryName("temperature"). \
                     start()

        except Exception as e:
                print(f"""{e}, quitting""")
                sys.exit(1)
            
        #print(result.status)
        #print(result.recentProgress)
        #print(result.lastProgress)

        result.awaitTermination()

if __name__ == "__main__":
    
    #appName = config['common']['appName']
    appName = "temperature"
    spark_session = s.spark_session(appName)
    spark_session = s.setSparkConfStreaming(spark_session)
    spark_session = s.setSparkConfBQ(spark_session)
    spark_context = s.sparkcontext()
    temperatureStreaming = temperatureStreaming(spark_session, spark_context)
    streamingDataFrame = temperatureStreaming.fetch_data()
