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
import uuid


# Takes in a StructType schema object and return a column selector that flattens the Struct
def flatten_struct(schema, prefix=""):
    result = []
    for elem in schema:
        if isinstance(elem.dataType, StructType):
            result += flatten_struct(elem.dataType, prefix + elem.name + ".")
        else:
            result.append(col(prefix + elem.name).alias(prefix + elem.name))
    return result


def sendToKafka(df, batchId):
    if(len(df.take(1))) > 0:
        print(f"""batchId is {batchId}""")
        df.show(100,False)
    else:
        print("DataFrame is empty")

class temperatureStreaming:
    def __init__(self,spark_session,spark_context):
        self.spark = spark_session
        self.sc = spark_context
        self.config = config

    def fetch_data(self):
        self.sc.setLogLevel("ERROR")
        schema = StructType() \
         .add("rowkey", StringType()) \
         .add("timestamp", TimestampType()) \
         .add("temperature", IntegerType())
        checkpoint_path = "file:///ssd/hduser/avgtemperature/chkpt"
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
                .option("startingOffsets", "earliest") \
                .load() \
                .select(from_json(col("value").cast("string"), schema).alias("parsed_value"))


            resultC = streamingDataFrame.select( \
                     col("parsed_value.rowkey").alias("rowkey") \
                   , col("parsed_value.timestamp").alias("timestamp") \
                   , col("parsed_value.temperature").alias("temperature"))

            """
            We work out the window and the AVG(temperature) in the window's timeframe below
            This should return back the following Dataframe as struct

             root
             |-- window: struct (nullable = false)
             |    |-- start: timestamp (nullable = true)
             |    |-- end: timestamp (nullable = true)
             |-- avg(temperature): double (nullable = true)

            """
            resultM = resultC. \
                     withWatermark("timestamp", "5 minutes"). \
                     groupBy(window(resultC.timestamp, "5 minutes", "5 minutes")). \
                     avg('temperature')

            # We take the above DataFrame and flatten it to get the columns aliased as "startOfWindowFrame", "endOfWindowFrame" and "AVGTemperature" 
            resultMF = resultM. \
                       select( \
                            F.col("window.start").alias("startOfWindow") \
                          , F.col("window.end").alias("endOfWindow") \
                          , F.col("avg(temperature)").alias("AVGTemperature"))

            # Kafka producer requires a key, value pair. We generate UUID key as the unique identifier of Kafka record
            uuidUdf= F.udf(lambda : str(uuid.uuid4()),StringType())
          
            """
            We take DataFrame resultMF containing temperature info and write it to Kafka. The uuid is serialized as a string and used as the key.
            We take all the columns of the DataFrame and serialize them as a JSON string, putting the results in the "value" of the record.
            """
            result = resultMF.withColumn("uuid",uuidUdf()) \
                     .selectExpr("CAST(uuid AS STRING) AS key", "to_json(struct(startOfWindow, endOfWindow, AVGTemperature)) AS value") \
                     .writeStream \
                     .outputMode('complete') \
                     .format("kafka") \
                     .option("kafka.bootstrap.servers", config['MDVariables']['bootstrapServers'],) \
                     .option("topic", "avgtemperature") \
                     .option('checkpointLocation', checkpoint_path) \
                     .queryName("avgtemperature") \
                     .start()

        except Exception as e:
                print(f"""{e}, quitting""")
                sys.exit(1)
            
        #print(result.status)
        #print(result.recentProgress)
        #print(result.lastProgress)

        result.awaitTermination()

if __name__ == "__main__":
    
    #appName = config['common']['appName']
    ## new appName
    # added a redundant line
    appName = "temperature"
    spark_session = s.spark_session(appName)
    spark_session = s.setSparkConfStreaming(spark_session)
    spark_session = s.setSparkConfBQ(spark_session)
    spark_context = s.sparkcontext()
    temperatureStreaming = temperatureStreaming(spark_session, spark_context)
    streamingDataFrame = temperatureStreaming.fetch_data()
