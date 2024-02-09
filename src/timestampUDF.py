from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, udf
from pyspark.sql.types import StringType

# Create a Spark session
spark = SparkSession.builder.appName("TimestampUDFExample").getOrCreate()

# Sample DataFrame
df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])

# Define a Python function to extract the timestamp part
def extract_timestamp_part(timestamp):
    # Customize this based on the part you want (e.g., 'yyyy-MM-dd HH:mm:ss')
    return timestamp.strftime('%Y-%m-%d %H:%M:%S')

# Create a UDF from the Python function
timestamp_udf = udf(extract_timestamp_part, StringType())

# Add a new column using the UDF
df = df.withColumn("op_time", timestamp_udf(current_timestamp()))

# Show the DataFrame with the new column
df.show(truncate=False)

# Stop the Spark session
spark.stop()


