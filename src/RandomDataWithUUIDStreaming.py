from __future__ import print_function
import sys
import json

# Import necessary modules and add paths
sys.path.append('/home/hduser/dba/bin/python/DSBQ/')
sys.path.append('/home/hduser/dba/bin/python/DSBQ/conf')
sys.path.append('/home/hduser/dba/bin/python/DSBQ/othermisc')
sys.path.append('/home/hduser/dba/bin/python/DSBQ/src')
sys.path.append('/home/hduser/.local/lib/python3.9/site-packages')
import findspark
findspark.init()
from config import config
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round, current_timestamp, lit
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.window import Window
from sparkutils import sparkstuff as s
from othermisc import usedFunctions as uf
import datetime
import uuid
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.functions import udf
import schedule


def main():
    # Set the application name
    appName = "RandomDataPysparkStreaming"

    # Initialize Spark session and context
    spark_session = s.spark_session(appName)
    spark_session = s.setSparkConfStreaming(spark_session)
    spark_session = s.setSparkConfBQ(spark_session)
    spark_context = s.sparkcontext()
    # Set the log level to ERROR to reduce verbosity
    spark_context.setLogLevel("ERROR")
    
    # Get start time
    lst = (spark_session.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at")
    uf.println(lst)

    # Create an instance of RandomDataStreaming class
    randomdataStreamingInstance = RandomDataStreaming(spark_session, spark_context)
    
    # Generate random data
    dfRandom = randomdataStreamingInstance.generateRandomData(appName)

    # Get finish time
    lst = (spark_session.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at")
    uf.println(lst)

    # Stop the Spark session
    spark_session.stop()

# Define the rates function outside of the class
# Spark expects rates to be a function that accepts a DataFrame and a batch ID as parameters


def rates(df: F.DataFrame, batchId: int) -> None:
    if(len(df.take(1))) > 0:
        df.select(col("timestamp"), col("value"), col("rowkey"), col("ID"), col("CLUSTERED"), col("op_time")).show(100, False)
    else:
        print("DataFrame is empty")


class RandomDataStreaming:

    def __init__(self, spark_session, spark_context):
        self.spark = spark_session
        self.sc = spark_context
        self.config = config

    def padSingleChar2(self, chars, length):
        # Pad a single character to a specified length
        result_str = F.concat_ws("", F.sequence(F.lit(1), F.lit(length), F.lit(1)), F.lit(chars).cast("string"))
        return result_str

    def readDataFromBQTable(self):
        # Read data from a BigQuery table
        dataset = "test"
        tableName = "randomdata"
        fullyQualifiedTableName = dataset + '.' + tableName
        read_df = s.loadTableFromBQ(self.spark, dataset, tableName)
        return read_df

    def getValuesFromBQTable(self):
        # Get row count and max ID from a BigQuery table
        read_df = self.readDataFromBQTable()
        read_df.createOrReplaceTempView("tmp_view")
        rows = self.spark.sql("SELECT COUNT(1) FROM tmp_view").collect()[0][0]
        maxID = self.spark.sql("SELECT MAX(ID) FROM tmp_view").collect()[0][0]
        return {"rows": rows, "maxID": maxID}

    def clustered2(col, numRows):
        # Apply clustering logic to a column
        return F.floor(col - 1) / numRows  # Using F.floor for rounding

    def generateRandomData(self, appName):
        numRows = 10
        processingTime = 2
        rows = 0
        values = self.getValuesFromBQTable()
        rows = values["rows"]
        maxID = values["maxID"]
        start = 0
        if rows == 0:
            start = 1
        else:
            start = maxID + 1
        end = start + numRows
        print("starting at ID = ", start, ",ending on = ", end)
        Range = range(start, end)
        # Kafka producer requires a key, value pair. We generate UUID key as the unique identifier of Kafka record
        # # This traverses through the Range and increment "x" by one unit each time, and that x value is used in the code to generate random data through Python functions in a class

        rdd = self.sc.parallelize(Range). \
            map(lambda x: (str(uuid.uuid4()),
                           x, \
                           uf.clustered(x, numRows), \
                           uf.scattered(x, numRows),
                           uf.randomised(x, numRows), \
                           uf.randomString(50),
                           uf.padString(x, " ", 50), \
                           uf.padSingleChar("x", 50)))
        # Convert RDD to DataFrame
        df = rdd.toDF(["KEY", "ID", "CLUSTERED", "SCATTERED", "RANDOMISED", "RANDOM_STRING", "SMALL_VC", "PADDING"])

        # Add metadata columns
        df = df.withColumn("op_type", lit(config['MDVariables']['op_type'])). \
            withColumn("op_time", current_timestamp())

        # define checkpoint directory
        checkpoint_path = "file:///ssd/hduser/randomdata/chkpt"

        try:

            # construct a streaming dataframe that subscribes to topic rate for data
            streamingDataFrame = self.spark \
                .readStream \
                .format("rate") \
                .option("rowsPerSecond", numRows) \
                .option("auto.commit.interval.ms", config['MDVariables']['autoCommitIntervalMS']) \
                .option("subscribe", "rate") \
                .option("failOnDataLoss", "false") \
                .option("includeHeaders", "true") \
                .option("startingOffsets", "latest") \
                .load() \
                .withColumn("KEY", F.lit(str(uuid.uuid4()))) \
                .withColumn("ID", F.col("value")) \
                .withColumn("CLUSTERED", uf.clustered2(F.col("value"), numRows)) \
                .withColumn("SCATTERED", F.abs((F.col("value") - 1) % numRows) * 1.0) \
                .withColumn("RANDOMISED", uf.randomised2(F.col("value"), numRows)) \
                .withColumn("RANDOM_STRING", udf(uf.randomString, StringType())(F.lit(50))) \
                .withColumn("SMALL_VC", uf.padString2(col("value") + 1, 50, " ")) \
                .withColumn("PADDING", self.padSingleChar2(lit("x"), lit(50))) \
                .withColumn("op_type", lit(config['MDVariables']['op_type'])) \
                .withColumn("op_time", F.current_timestamp())

            """
            In the context of Spark Structured Streaming with the "rate" source,
            the schema of the streaming DataFrame includes two columns:

            - timestamp: This column contains the timestamp associated with each generated record. It represents the time at which the record was generated.
            - value: This column contains a long integer value associated with each record.          
            """
            streamingDataFrame.printSchema()

            result = streamingDataFrame.select(\
                col("timestamp").alias("timestamp") \
                , col("value").alias("value") \
                , col("KEY").alias("rowkey") \
                , col("ID").alias("ID") \
                , col("CLUSTERED").alias("CLUSTERED") \
                , col("RANDOMISED").alias("RANDOMISED") \
                , col("RANDOM_STRING").alias("RANDOM_STRING") \
                , col("SMALL_VC").alias("SMALL_VC") \
                , col("PADDING").alias("PADDING") \
                , col("RANDOM_STRING").alias("RANDOM_STRING") \
                , col("op_type").alias("op_type") \
                , col("op_time").alias("op_time")). \
                writeStream. \
                outputMode('append'). \
                option("truncate", "false"). \
                foreachBatch(lambda df, batchId: rates(df, batchId)). \
                trigger(processingTime=f'{processingTime} seconds'). \
                option('checkpointLocation', checkpoint_path). \
                queryName(f"{appName}"). \
                start()
            # print(result)

        except Exception as e:
            print(f"{str(e)}, quitting!")
            sys.exit(1)

        self.spark.streams.awaitAnyTermination()
        result.awaitTermination()


if __name__ == "__main__":
    main()

