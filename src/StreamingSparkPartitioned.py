from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, when
from pyspark.sql.types import StructType, StructField, LongType
from datetime import datetime

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("StreamingSparkPartitioned") \
    .getOrCreate()

expression = when(expr("value % 3 = 1"), "stupid_event") \
    .otherwise(when(expr("value % 3 = 2"), "smart_event").otherwise("neutral_event"))

# Define the schema to match the rate-micro-batch data source
schema = StructType([StructField("timestamp", LongType()), StructField("value", LongType())])
checkpoint_path = "file:///ssd/hduser/randomdata/chkpt"

# Convert human-readable timestamp to Unix timestamp in milliseconds
start_timestamp = int(datetime(2024, 1, 28).timestamp() * 1000)

streamingDF = spark.readStream \
    .format("rate-micro-batch") \
    .option("rowsPerBatch", "100") \
    .option("startTimestamp", start_timestamp) \
    .option("numPartitions", 1) \
    .load() \
    .withColumn("event_type", expression)

query = (
    streamingDF.writeStream
    .outputMode("append")
    .format("console")
    .trigger(processingTime="1 second")
    .option("checkpointLocation", checkpoint_path)
    .start()
)

query.awaitTermination()
