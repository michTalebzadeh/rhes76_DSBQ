from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Create a Spark session
spark = SparkSession.builder.appName("BroadcastExample").getOrCreate()

# Sample data for a large read-only dataset
large_data = [("A", 1), ("B", 2), ("C", 3)]
large_df = spark.createDataFrame(large_data, ["key", "value"])

# Another DataFrame with data to be joined
input_data = [("A", "data1"), ("B", "data2"), ("C", "data3")]
input_df = spark.createDataFrame(input_data, ["key", "data"])

# Use broadcast for the join operation
joined_df = input_df.join(broadcast(large_df), "key")

joined_df.explain("extended")

# Show the result
joined_df.show()


