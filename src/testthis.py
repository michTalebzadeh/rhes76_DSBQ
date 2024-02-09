from __future__ import print_function
import sys
import json
import findspark
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

sys.path.append('/home/hduser/dba/bin/python/DSBQ/')
sys.path.append('/home/hduser/dba/bin/python/DSBQ/conf')
sys.path.append('/home/hduser/dba/bin/python/DSBQ/othermisc')
sys.path.append('/home/hduser/dba/bin/python/DSBQ/src')

findspark.init()

# Create a Spark session
spark = SparkSession.builder.appName("Test").getOrCreate()

@udf("string")  # Specify the return type
def padSingleChar2(chars, length):
    result_str = ''.join(chars for _ in range(length))
    return result_str

# Sample data
data = [(1,), (2,), (3,)]
columns = ["value"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Apply the function to a column
result_df = df.withColumn("PADDING", padSingleChar2(lit("x"), lit(50)))

# Show the result
result_df.show()


