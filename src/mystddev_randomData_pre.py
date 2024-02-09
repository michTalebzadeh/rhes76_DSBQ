"""
In this brief Python code tried on PySpark 3.4.0, we generate STDDEV function using
- Standard function from PySpark
- through UDF
- Manually
"""
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import stddev, col, avg, udf, collect_list
from pyspark.sql.types import StructType, StructField,  FloatType
import math
import random
import string

def randomString(length):
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def clustered(x,numRows):
    return math.floor(x -1)/numRows

def scattered(x,numRows):
    return abs((x -1 % numRows))* 1.0

def randomised(seed,numRows):
    random.seed(seed)
    return abs(random.randint(0, numRows) % numRows) * 1.0

def padString(x,chars,length):
    n = int(math.log10(x) + 1)
    result_str = ''.join(random.choice(chars) for i in range(length-n)) + str(x)
    return result_str

def padSingleChar(chars,length):
    result_str = ''.join(chars for i in range(length))
    return result_str

def println(lst):
    for ll in lst:
      print(ll[0])

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
  sc = SparkContext.getOrCreate()
  sc.setLogLevel("ERROR")

  # Create a DataFrame with rowsToGenerate random values (replace this with your actual data)
  rowsToGenerate = 10000
  start = 1
  end = start + rowsToGenerate - 1
  print ("starting at ID = ",start, ",ending on = ",end)
  Range = range(start, end+1)
  ## This traverses through the Range and increment "x" by one unit each time, and that x value is used in the code to generate random data through Python lambda function

  rdd = sc.parallelize(Range). \
         map(lambda x: (x, clustered(x,rowsToGenerate), \
                           scattered(x,rowsToGenerate), \
                           randomised(x, rowsToGenerate), \
                           randomString(50), \
                           padString(x," ",50), \
                           padSingleChar("x",50)))
  df = rdd.toDF(). \
         withColumnRenamed("_1","ID"). \
         withColumnRenamed("_2", "CLUSTERED"). \
         withColumnRenamed("_3", "SCATTERED"). \
         withColumnRenamed("_4", "RANDOMISED"). \
         withColumnRenamed("_5", "RANDOM_STRING"). \
         withColumnRenamed("_6", "SMALL_VC"). \
         withColumnRenamed("_7", "PADDING")
  df.printSchema()
  # Calculate the average value and count once

  # Register the UDF
  stddev_udf = udf(calculate_stddev, FloatType())
  # Calculate the standard deviation of "RANDOMISED" column
  std_udf = df.agg(stddev_udf(collect_list("RANDOMISED"))).alias("stddev").collect()[0][0]

  # Calculate the standard deviation using PySpark's stddev function
  std_dev_pyspark = df.select(stddev(col("RANDOMISED"))).collect()[0][0]

  # Manually calculate the standard deviation
  avg_value = df.select(avg(col("RANDOMISED"))).collect()[0][0]
  squared_diff = df.select(col("RANDOMISED")).rdd.map(lambda x: (x[0] - avg_value) ** 2)
  variance = squared_diff.sum() / df.count()
  std_dev_manual = math.sqrt(variance)

  # Display both standard deviations
  print("\nStandard Deviation (PySpark):", std_dev_pyspark)
  print("\nManually Calculated Standard Deviation from udf:", std_udf)
  print("\nManually Calculated Standard Deviation:", std_dev_manual)
  print(f"""\nThese results sampled on {rowsToGenerate} rows may be slightly different due to floating-point precision, but they should be very close""")

if __name__ == "__main__":
    main()
