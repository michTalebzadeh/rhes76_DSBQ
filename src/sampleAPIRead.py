"""
The PySpark code below sets up a Spark Streaming application that reads data from an API, processes it in batches,
and outputs the results to the console. It uses Spark's structured streaming capabilities, 
allowing one to handle real-time data in a scalable manner.
"""
import sys
import json

# Import necessary modules and add paths
sys.path.append('/home/hduser/dba/bin/python/DSBQ/')
sys.path.append('/home/hduser/dba/bin/python/DSBQ/conf')
sys.path.append('/home/hduser/dba/bin/python/DSBQ/othermisc')
sys.path.append('/home/hduser/dba/bin/python/DSBQ/src')
sys.path.append('/home/hduser/dba/bin/python/DSBQ/udfs')
sys.path.append('/home/hduser/.local/lib/python3.9/site-packages')
from config import config
from udfs import udf_functions as udfs
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
appName = "sampleAPIRead"

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from flask import Flask, request
from flask_restful import Resource, Api
import threading

app = Flask(__name__)
api = Api(app)

spark_session = s.spark_session(appName)
spark_session = s.setSparkConfStreaming(spark_session)
spark_session = s.setSparkConfBQ(spark_session)
spark_context = s.sparkcontext()

# Set the log level to ERROR to reduce verbosity
spark_context.setLogLevel("ERROR")
class StartStreamingResource(Resource):
    def __init__(self, spark_session, spark_context, streaming_dataframe, processing_time, checkpoint_path):
        self.spark_session = spark_session
        self.spark_context = spark_context
        self.streaming_dataframe = streaming_dataframe
        self.processing_time = processing_time
        self.checkpoint_path = checkpoint_path
        self.query = None
        super().__init__()

    def post(self):
        # Start the streaming query
        self.start_streaming_query()
        return {'status': 'Streaming started.'}

    def start_streaming_query(self):
        if self.query is None or self.query.isActive is False:
            # Start streaming only if it's not already active
            query_name = f"StreamingQuery_{int(time.time())}"
            logging.info(query_name)
            self.query = (
                self.streaming_dataframe.writeStream
                    .outputMode('append')
                    .option("truncate", "false")
                    .foreachBatch(lambda df, batchId: process_data(df, batchId))
                    .trigger(processingTime=f'{self.processing_time} seconds')
                    .option('checkpointLocation', self.checkpoint_path)
                    .queryName(f"{query_name}")
                    .start()
            )

class StopStreamingResource(Resource):
    def __init__(self, query):
        self.query = query
        super().__init__()

    def get(self):
        # Stop the streaming query
        self.stop_streaming_query()
        return {'status': 'Streaming stopped.'}

    def stop_streaming_query(self):
        if self.query is not None and self.query.isActive:
            # Stop streaming only if it's active
            self.query.stop()

api.add_resource(
    StartStreamingResource,
    '/start_streaming',
    resource_class_args=(spark_session, spark_context, streamingDataFrame, processingTime, checkpoint_path),
    resource_class_kwargs={}
)

api.add_resource(
    StopStreamingResource,
    '/stop_streaming',
    resource_class_args=(result_query,),
    resource_class_kwargs={}
)

if __name__ == '__main__':
    app.run(port=5000, debug=True)


spark_session = s.spark_session(appName)
spark_session = s.setSparkConfStreaming(spark_session)
spark_session = s.setSparkConfBQ(spark_session)
spark_context = s.sparkcontext()

# Set the log level to ERROR to reduce verbosity
spark_context.setLogLevel("ERROR")

numRows = 100
processingTime = 5 
 # define checkpoint directory
checkpoint_path = "file:///ssd/hduser/randomdata/chkpt"
   
# Schema Definition:
"""
This part defines the schema for the DataFrame that will be created from the API data. 
It specifies the structure of the data, including column names and their corresponding data types.
"""
data_schema = StructType([
    StructField("rowkey", StringType()),
    StructField("ticker", StringType()),
    StructField("timeissued", StringType()),
    StructField("price", FloatType())
])

# API Data Retrieval:
"""
This function makes an HTTP GET request to the specified API endpoint
(http://rhes75:8999/api/data). 
It handles potential errors such as failed requests or JSON decoding errors.
"""
def get_api_data():
    try:
        response = requests.get("http://rhes75:8999/api/data")
        response.raise_for_status()  # Raise an HTTPError for bad responses
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None

# External event trigger setup (replace with your actual mechanism)
def listen_for_external_event():
    # Your logic to listen for an external event, e.g., Kafka consumer, file watcher, HTTP server, etc.
    return True

    # Modified process_data function to check for external trigger
def process_data(batch_df: F.DataFrame, batchId: int) -> None:
    if len(batch_df.take(1)) > 0:
      # Check for external event trigger
      if listen_for_external_event():
        # Assuming 'data' is a list of dictionaries obtained from the API in each batch
        api_data = get_api_data()
        if api_data:
          dfAPI = spark_session.createDataFrame(api_data, schema=data_schema)
          dfAPI = dfAPI \
             .withColumn("op_type", lit(udfs.op_type_api_udf())) \
             .withColumn("op_time", udfs.timestamp_udf(current_timestamp()))

          dfAPI.show(10, False)
        else:
           logging.warning("Error getting API data.")
      else:
        logging.info("No external trigger received.")
    else:
        logging.warning("DataFrame is empty")

# Streaming DataFrame Creation:
# construct a streaming dataframe that subscribes to topic rate for data
"""
This creates a streaming DataFrame by subscribing to a rate source.
It simulates a stream by generating data at a specified rate (rowsPerSecond).
"""
streamingDataFrame = spark_session.readStream.format("rate") \
    .option("rowsPerSecond", 100) \
    .option("subscribe", "rate") \
    .option("failOnDataLoss", "false") \
    .option("includeHeaders", "true") \
    .option("startingOffsets", "latest") \
    .load()

# Generate a unique query name by appending a timestamp
query_name = f"{appName}_{int(time.time())}"
logging.info(query_name)

    # Main loop to continuously check for events
while True:
    # Start the streaming query only if an external event is received
    if listen_for_external_event():
        query_name = f"{appName}_{int(time.time())}"
        logging.info(query_name)
        result_query = (
            streamingDataFrame.writeStream
                .outputMode('append')
                .option("truncate", "false")
                .foreachBatch(lambda df, batchId: process_data(df, batchId))
                .trigger(processingTime=f'{processingTime} seconds')
                .option('checkpointLocation', checkpoint_path)
                .queryName(f"{query_name}")
                .start()
        )
        break  # Exit the loop after starting the streaming query
    else:
        time.sleep(5)  # Sleep for a while before checking for the next event

# Wait for the termination of the streaming query
spark_session.streams.awaitAnyTermination()

# Finally, when the streaming query terminates, it stops the Spark session and context.
spark_session.stop()
spark_context.stop()

