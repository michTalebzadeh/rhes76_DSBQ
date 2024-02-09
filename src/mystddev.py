"""
In this brief Python code tried on PySpark 3.4.0, we generate STDDEV function using
- Standard function from PySpark
- through UDF
- Manually
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, col, avg, udf, collect_list
from pyspark.sql.types import StructType, StructField,  FloatType
import math

# Define the UDF function to create STDDEV
def calculate_stddev(values):
    n = len(values)
    avg_value = sum(values) / n
    squared_diff = [(x - avg_value) ** 2 for x in values]
    variance = sum(squared_diff) / n
    std_dev = math.sqrt(variance)
    return std_dev

def main():
 
  # Initialize a Spark session
  spark = SparkSession.builder.appName("StandardDeviationComparison").getOrCreate()

  # Create a DataFrame with num_rows random values (replace this with your actual data)
  num_rows = 10000
  data = [(float(i),) for i in range(1, num_rows + 1)]
  # Define your schema
  Schema = StructType([StructField("RANDOMISED", FloatType(), True)])
  df = spark.createDataFrame(data, schema=Schema)  
  # Calculate the average value and count once

  # Register the UDF
  stddev_udf = udf(calculate_stddev, FloatType())
  # Calculate the standard deviation of "RANDOMISED" column
  std_udf = df.agg(stddev_udf(collect_list("RANDOMISED"))).alias("stddev").collect()[0][0]

  # Calculate the standard deviation using PySpark's stddev function
  std_dev_pyspark = df.select(stddev(col("RANDOMISED"))).collect()[0][0]

  # Manually calculate the standard deviation
  n = num_rows
  avg_value = df.select(avg(col("RANDOMISED"))).collect()[0][0]
  squared_diff = df.select(col("RANDOMISED")).rdd.map(lambda x: (x[0] - avg_value) ** 2)
  variance = squared_diff.sum() / n
  std_dev_manual = math.sqrt(variance)

  # Display both standard deviations
  print("\nStandard Deviation (PySpark):", std_dev_pyspark)
  print("\nManually Calculated Standard Deviation from udf:", std_udf)
  print("\nManually Calculated Standard Deviation:", std_dev_manual)
  print(f"""\nThese results sampled on {n} rows may be slightly different due to floating-point precision, but they should be very close""")

if __name__ == "__main__":
    main()
