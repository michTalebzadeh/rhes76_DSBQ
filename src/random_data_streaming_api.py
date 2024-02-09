from __future__ import print_function
import sys
import json

# Import necessary modules and add paths
sys.path.append('/home/hduser/dba/bin/python/DSBQ/')
sys.path.append('/home/hduser/dba/bin/python/DSBQ/conf')
sys.path.append('/home/hduser/dba/bin/python/DSBQ/othermisc')
sys.path.append('/home/hduser/dba/bin/python/DSBQ/src')
sys.path.append('/home/hduser/.local/lib/python3.9/site-packages')
from config import config
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round, current_timestamp, lit
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.window import Window
from sparkutils import sparkstuff as s
from othermisc import usedFunctions as uf
import datetime
import time
import uuid
from pyspark.sql.functions import udf
import schedule

from flask.signals import got_request_exception
from pyspark.sql.streaming import DataStreamWriter
import socket
from flask import Flask
from flask_restful import Resource, Api
import logging
import pyspark

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
appName = "RandomDataPysparkStreaming"

spark_session = s.spark_session(appName)
spark_session = s.setSparkConfStreaming(spark_session)
spark_session = s.setSparkConfBQ(spark_session)
spark_context = s.sparkcontext()

# Set the log level to ERROR to reduce verbosity
spark_context.setLogLevel("ERROR")

app = Flask(__name__)
api = Api(app)

class RandomDataResource(Resource):
    def __init__(self, spark_session, spark_context):
        # Use the provided Spark session and Spark context
        self.spark_session = spark_session
        self.spark_context = spark_context
        
        # Define result_query as an instance variable
        self.result_query = None

        super(RandomDataResource, self).__init__()
        
    def stop_streaming(self):
        # Access result_query and check if it's active
        if self.result_query and self.result_query.isActive:
            self.result_query.stop()
            logging.info("Streaming stopped gracefully")
        else:
            logging.info("No active streaming query")
                
    def padSingleChar2(self, chars, length):
        # Pad a single character to a specified length
        result_str = F.concat_ws("", F.sequence(F.lit(1), F.lit(length), F.lit(1)), F.lit(chars).cast("string"))
        return result_str

    def rates(self, df: F.DataFrame, batchId: int) -> None:
      if(len(df.take(1))) > 0:
        df.select(col("timestamp"), col("value"), col("rowkey"), col("ID"), col("CLUSTERED"), col("op_time")).show(1, False)
        df.createOrReplaceTempView("tmp_view")
        try:
           rows = df.sparkSession.sql("SELECT COUNT(1) FROM tmp_view").collect()[0][0]
           print(f"Number of rows: {rows}")
        except Exception as e:
           logging.error(f"Error counting rows: {e}")
      else:
        logging.warning("DataFrame is empty")

    def generate_random_data(self, numRows):

      numRows = 10
      processingTime = 5 
      rows = 0
      maxID = numRows
      start = 0
      if rows == 0:
        start = 1
      else:
        start = maxID + 1
      end = start + numRows
      logging.info("starting at ID = ", start, ",ending on = ", end)
      Range = list(range(start, end))
      
      rdd = self.spark_context.parallelize(Range). \
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
            streamingDataFrame = self.spark_session \
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
                     
            result = streamingDataFrame.select(
                col("timestamp").alias("timestamp"),
                col("value").alias("value"),
                col("KEY").alias("rowkey"),
                col("ID").alias("ID"),
                col("CLUSTERED").alias("CLUSTERED"),
                col("RANDOMISED").alias("RANDOMISED"),
                col("RANDOM_STRING").alias("RANDOM_STRING"),
                col("SMALL_VC").alias("SMALL_VC"),
                col("PADDING").alias("PADDING"),
                col("RANDOM_STRING").alias("RANDOM_STRING"),
                col("op_type").alias("op_type"),
                col("op_time").alias("op_time")
                )
            # Generate a unique query name by appending a timestamp
            query_name = f"{appName}_{int(time.time())}"
            logging.info(query_name)
        
            # Access result_query and check if it's active
            if self.result_query and self.result_query.isActive:
              logging.warning("Streaming query is already active")
            else:
              # Start a new streaming query
              self.result_query = (
                    result.writeStream
                    .outputMode('append')
                    .option("truncate", "false")
                    .foreachBatch(lambda df, batchId: self.rates(df, batchId))
                    .trigger(processingTime=f'{processingTime} seconds')
                    .option('checkpointLocation', checkpoint_path)
                    .queryName(f"{query_name}")
                    .start()
                )
      except Exception as e:
            logging.error("%s, quitting!", str(e))
            sys.exit(1)
             
    def get(self):
        # Handle the GET request here
        return self.generate_random_data(numRows=10)

    def post(self):
        # Trigger the stop_streaming method in the RandomDataResource
        random_data_resource.stop_streaming()
        return {'message': 'Streaming stopped gracefully'}
    
    def create_streaming_dataframe(self, numRows):
        return self.spark_session.readStream.format("rate") \
            .option("rowsPerSecond", numRows) \
            .option("auto.commit.interval.ms", config['MDVariables']['autoCommitIntervalMS']) \
            .option("subscribe", "rate") \
            .option("failOnDataLoss", "false") \
            .option("includeHeaders", "true") \
            .option("startingOffsets", "latest") \
            .load() \
            .withColumn("KEY", lit(str(uuid.uuid4()))) \
            .withColumn("ID", col("value")) \
            .withColumn("CLUSTERED", uf.clustered2(col("value"), numRows)) \
            .withColumn("SCATTERED", (col("value") - 1) % numRows * 1.0) \
            .withColumn("RANDOMISED", uf.randomised2(col("value"), numRows)) \
            .withColumn("RANDOM_STRING", udf(uf.randomString, StringType())(lit(50))) \
            .withColumn("SMALL_VC", uf.padString2(col("value") + 1, 50, " ")) \
            .withColumn("PADDING", uf.padSingleChar2(lit("x"), lit(50))) \
            .withColumn("op_type", lit(config['MDVariables']['op_type'])) \
            .withColumn("op_time", current_timestamp())

    def start_streaming(self, df, appName, numRows, processingTime):
        df.printShema()
        df.writeStream \
            .outputMode('append') \
            .option("truncate", "false") \
            .foreachBatch(lambda df, batchId: self.rates(df, batchId)) \
            .trigger(processingTime=f'{processingTime} seconds') \
            .option('checkpointLocation', self.checkpoint_path) \
            .queryName(f"{appName}") \
            .start()
   
        self.spark_session.streams.awaitAnyTermination()
  
    def getValuesFromBQTable(self):
        # Get row count and max ID from a BigQuery table
        read_df = self.readDataFromBQTable()
        read_df.createOrReplaceTempView("tmp_view")
        rows = self.spark_session.sql("SELECT COUNT(1) FROM tmp_view").collect()[0][0]
        maxID = self.spark_session.sql("SELECT MAX(ID) FROM tmp_view").collect()[0][0]
        return {"rows": rows, "maxID": maxID}
    
    def readDataFromBQTable(self):
        # Read data from a BigQuery table
        
        dataset = "test"
        tableName = "randomdata"
        fullyQualifiedTableName = dataset + '.' + tableName
        read_df = s.loadTableFromBQ(self.spark_session, dataset, tableName)
        return read_df

class StopStreamingResource(Resource):
    def __init__(self, spark_session, spark_context):
        self.spark_session = spark_session
        self.spark_context = spark_context
        # Create an instance of RandomDataResource
        self.random_data_resource = RandomDataResource(spark_session,spark_context)

    def post(self):
        # Trigger the stop_streaming method in the RandomDataResource
         self.random_data_resource.stop_streaming()

    def get(self):
        active = self.spark_session.streams.active
        for e in active:
             logging.info("Streaming queue is %s, going to stop it gracefully", e)
             e.stop()
        self.random_data_resource.stop_streaming()
        return {'message': 'Streaming stopped gracefully'}

# API endpoints for starting and stopping streaming
api.add_resource(
    RandomDataResource,
    '/start_streaming',
    resource_class_args=(spark_session, spark_context),
    resource_class_kwargs={}
)

api.add_resource(
    StopStreamingResource,
    '/stop_streaming',
    methods=['GET', 'POST'],
    resource_class_args=(spark_session, spark_context),
    resource_class_kwargs={}
)

# Handle root path
@app.route('/')
def home():
    return 'Welcome to the Random Data Streaming API with Flask!'

# Handle favicon.ico request
@app.route('/favicon.ico')
def favicon():
    # You can return an actual favicon file or just a placeholder response
    return ''
      
def stop_spark_session(sender, exception, **extra):
    # Stop the Spark session when the Flask app is shutting down
    if sender and hasattr(sender, 'spark_session'):
        try:
            sender.spark_session.stop()
        except Exception as e:
            logging.error("An error occurred while stopping Spark session: %s", str(e))
            
if __name__ == '__main__':
    # Attach the signal for Flask app shutdown
    got_request_exception.connect(stop_spark_session, app)

    # Explicitly set the hostname and port
    host = socket.gethostname() # Allows external connections
    port = 7999

    # Run the Flask app
    app.run(debug=False, host=host, port=port)
    
    # Stop the Spark session & spark_context when Flask app exits
    spark_session.stop()
    spark_context.stop()
