from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("UDFExample").getOrCreate()

# Sample DataFrame
data = [(1,), (2,), (3,)]
columns = ["number"]
df = spark.createDataFrame(data, columns)

# Define the UDF function
def square_udf(number):
    return number ** 2

# Register the UDF with Spark
square_udf_spark = udf(square_udf, IntegerType())

# Use the UDF in a DataFrame transformation
df_squared = df.withColumn("number_squared", square_udf_spark(df["number"]))

# Show the result
df_squared.show()
