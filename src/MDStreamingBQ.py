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


def stopStreamQuery(query, awaitTerminationTimeMs):
    while (query.isActive):
      try:
        if(query.lastProgress.numInputRows < 10):
          query.awaitTermination(1000)
      except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)
        #case e:NullPointerException => println("First Batch")
        #Thread.sleep(500)


def SendToBigQuery(df, batchId):
    
    if(len(df.take(1))) > 0:
        #df.printSchema()
        df. persist()
        # read from redis table
        spark_session = s.spark_session(config['common']['appName'])
        spark_session = s.setSparkConfBQ(spark_session)
        # read from BigQuery
        read_df = s.loadTableFromBQ(spark_session, config['MDVariables']['targetDataset'],config['MDVariables']['targetTable'])
        #read_df = s.loadTableFromRedis(spark_session, config['RedisVariables']['targetTable'], config['RedisVariables']['keyColumn'])
        # Write data to config['MDVariables']['targetTable'] in BigQuery
        # look for high value tickers
        for row in df.rdd.collect():
            rowkey = row.rowkey
            ticker = row.ticker
            price = row.price
            values = bigQueryAverages(ticker,price,read_df)
            Average = values["average"]
            standardDeviation = values["standardDeviation"]
            lower = values["lower"]
            upper = values["upper"]
            if lower is not None and upper is not None:
              hvTicker = priceComparison(ticker,price,lower,upper)
              if(hvTicker == 1):
                 writeHighValueData(df,rowkey)
        df.unpersist()
    else:
        print("DataFrame is empty")

def bigQueryAverages(ticker,price,read_df):
        # get 14 movement average price for this ticker type
        # If no minimum data, then return nothing
        df2 = read_df.filter(col("ticker") == ticker).sort(col("timeissued").desc()).limit(config['MDVariables']['movingAverages'])
        if(df2.count()) >= 14: 
            #df2.show(14,False)
            Average = df2.select(avg(col("price"))).collect()[0][0]
            standardDeviation = df2.select(stddev(col("price"))).collect()[0][0]
            lower = Average - config['MDVariables']['confidenceLevel'] * standardDeviation
            upper = Average + config['MDVariables']['confidenceLevel'] * standardDeviation
            return {"average": Average,"standardDeviation": standardDeviation,"lower": lower,"upper": upper}
        else:
            print(f"""Not enough data for ticker {ticker}, skipping""")
            return {"average": None,"standardDeviation": None,"lower": None,"upper": None}

def priceComparison(ticker,price,lower,upper):
    hvTicker = 0
    #print("ticker = ",ticker,"price = ",price, "lower = ",lower,"upper = ",upper)
    if(price >= upper):
        print(f"""\n*** price for ticker {ticker} is {price:.2f} up by one SD  >=  {upper:.2f} SELL ***""")
        hvTicker = 1
    if(price <= lower):
        print(f"""\n*** price for ticker {ticker} is {price:.2f} <= {upper:.2f} down by one SD BUY ***""")
        hvTicker = 1
    return hvTicker

def writeHighValueData(df,rowkey):
        dfHighValue = df.filter(col("rowkey") == rowkey).select( \
                 col("rowkey") \
               , col("ticker") \
               , col("timeissued") \
               , col("price")). \
                 withColumn("currency", lit(config['MDVariables']['currency'])). \
                 withColumn("op_type", lit(config['MDVariables']['op_type'])). \
                 withColumn("op_time", current_timestamp())
    
        s.writeTableToBQ(dfHighValue, config['MDVariables']['mode'], config['MDVariables']['targetDataset'], config['MDVariables']['targetSpeedTable'])
    
        
class MDStreaming:
    def __init__(self,spark_session,spark_context):
        self.spark = spark_session
        self.sc = spark_context
        self.config = config

    def fetch_data(self):
        self.sc.setLogLevel("ERROR")
        #{"rowkey":"c9289c6e-77f5-4a65-9dfb-d6b675d67cff","ticker":"MSFT", "timeissued":"2021-02-23T08:42:23", "price":31.12}
        schema = StructType().add("rowkey", StringType()).add("ticker", StringType()).add("timeissued", TimestampType()).add("price", FloatType())
        try:
            # construct a streaming dataframe streamingDataFrame that subscribes to topic config['MDVariables']['topic']) -> md (market data)
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
                .option("subscribe", config['MDVariables']['topic']) \
                .option("failOnDataLoss", "false") \
                .option("includeHeaders", "true") \
                .option("startingOffsets", "latest") \
                .load() \
                .select(from_json(col("value").cast("string"), schema).alias("parsed_value"))

            #streamingDataFrame.printSchema()

            """   
               "foreach" performs custom write logic on each row and "foreachBatch" performs custom write logic on each micro-batch through SendToBigQuery function
                foreachBatch(SendToBigQuery) expects 2 parameters, first: micro-batch as DataFrame or Dataset and second: unique id for each batch
               Using foreachBatch, we write each micro batch to storage defined in our custom logic. In this case, we store the output of our streaming application to Google BigQuery table.
               Note that we are appending data and column "rowkey" is defined as UUID so it can be used as the primary key
            """
            result = streamingDataFrame.select( \
                     col("parsed_value.rowkey").alias("rowkey") \
                   , col("parsed_value.ticker").alias("ticker") \
                   , col("parsed_value.timeissued").alias("timeissued") \
                   , col("parsed_value.price").alias("price")). \
                     writeStream. \
                     outputMode('append'). \
                     option("truncate", "false"). \
                     foreachBatch(SendToBigQuery). \
                     trigger(processingTime='2 seconds'). \
                     start()
        except Exception as e:
                print(f"""{e}, quitting""")
                sys.exit(1)
            
        #print(result.status)
        #print(result.recentProgress)
        #print(result.lastProgress)

        #stopStreamQuery(result,3000)
        result.awaitTermination(3000)

if __name__ == "__main__":
    
    appName = config['common']['appName']
    spark_session = s.spark_session(appName)
    spark_session = s.setSparkConfStreaming(spark_session)
    spark_session = s.setSparkConfBQ(spark_session)
    spark_context = s.sparkcontext()
    mdstreaming = MDStreaming(spark_session, spark_context)
    streamingDataFrame = mdstreaming.fetch_data()
