import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import stddev, col, avg, udf, collect_list
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
import math
import random
import string
"""
Generate test code in Python fot standard deviation
"""

def main():
    # Get the number of rows to generate from command-line arguments
    rows_to_generate = int(sys.argv[1])  # Parameter passed as (i)
    
    # Define the Spark application name
    appName = "StandardDeviationComparison"
    
    # Starting index for data generation
    start = 1
    
    # Initialize Spark session and context
    spark_session = SparkSession.builder.appName(appName).getOrCreate()
    spark_context = SparkContext.getOrCreate()
    
    # Set the log level to ERROR to reduce verbosity
    spark_context.setLogLevel("ERROR")
    
    # Create an instance of the SparkStandardDeviationCalculator class
    calculator = SparkStandardDeviationCalculator(spark_session, spark_context)
    
    # Run the standard deviation calculation
    calculator.run(rows_to_generate=rows_to_generate, start=start)

class SparkStandardDeviationCalculator:
    _instance = None  # Class-level variable to store the singleton instance

    def __init__(self, spark_session, spark_context):
        self.spark = spark_session
        self.sc = spark_context

    @classmethod
    def get_instance(cls, spark_session, spark_context):
        if cls._instance is None:
            cls._instance = cls(spark_session, spark_context)
        return cls._instance

    def generate_data(self, rows_to_generate, start):
        # Calculate the ending index for data generation
        end = start + rows_to_generate - 1
        print("Starting at ID =", start, ", ending on =", end)
        
        # Generate data for each row
        data = [(x, self.clustered(x, rows_to_generate), self.scattered(x, rows_to_generate),
                 self.randomised(x, rows_to_generate), self.random_string(50), self.pad_string(x, " ", 50),
                 self.pad_single_char("x", 50)) for x in range(start, end + 1)]
        
        # Create a DataFrame from the generated data using the specified schema
        return self.spark.createDataFrame(data, self.get_data_schema())

    def get_data_schema(self):
        # Define the schema for the DataFrame
        return StructType([
            StructField("ID", IntegerType(), True),
            StructField("CLUSTERED", FloatType(), True),
            StructField("SCATTERED", FloatType(), True),
            StructField("RANDOMISED", FloatType(), True),
            StructField("RANDOM_STRING", StringType(), True),
            StructField("SMALL_VC", StringType(), True),
            StructField("PADDING", StringType(), True)
        ])

    @staticmethod
    def random_string(length):
        # Generate a random string of letters
        letters = string.ascii_letters
        return ''.join(random.choice(letters) for _ in range(length))

    @staticmethod
    def clustered(x, num_rows):
        # Perform a mathematical operation
        return math.floor(x - 1) / num_rows

    @staticmethod
    def scattered(x, num_rows):
        # Perform a mathematical operation
        return abs((x - 1 % num_rows)) * 1.0

    @staticmethod
    def randomised(seed, num_rows):
        # Generate a random number based on a seed
        random.seed(seed)
        return abs(random.randint(0, num_rows) % num_rows) * 1.0

    @staticmethod
    def pad_string(x, chars, length):
        # Pad a string with characters
        n = int(math.log10(x) + 1)
        return ''.join(random.choice(chars) for _ in range(length - n)) + str(x)

    @staticmethod
    def pad_single_char(chars, length):
        # Generate a string consisting of a single character repeated multiple times
        return ''.join(chars for _ in range(length))

    @staticmethod
    def calculate_stddev(values):
        # Calculate the standard deviation of a list of values
        n = len(values)
        avg_value = sum(values) / n
        squared_diff = [(x - avg_value) ** 2 for x in values]
        variance = sum(squared_diff) / n
        std_dev = math.sqrt(variance)
        return std_dev

    def calculate_standard_deviation(self, df):
        # Register a UDF for calculating standard deviation
        stddev_udf = udf(self.calculate_stddev, FloatType())
        
        # Calculate standard deviation using PySpark's stddev function and manual calculation
        std_udf = df.agg(stddev_udf(collect_list("RANDOMISED"))).alias("stddev").collect()[0][0]
        std_dev_pyspark = df.select(stddev(col("RANDOMISED"))).collect()[0][0]
        avg_value = df.select(avg(col("RANDOMISED"))).collect()[0][0]
        squared_diff = df.select(col("RANDOMISED")).rdd.map(lambda x: (x[0] - avg_value) ** 2)
        variance = squared_diff.sum() / df.count()
        std_dev_manual = math.sqrt(variance)

        return std_dev_pyspark, std_udf, std_dev_manual

    def run(self, rows_to_generate, start):
        # Generate data and calculate standard deviation
        df = self.generate_data(rows_to_generate, start)
        std_dev_pyspark, std_udf, std_dev_manual = self.calculate_standard_deviation(df)

        # Display the results
        print("\nStandard Deviation (PySpark):", std_dev_pyspark)
        print("\nManually Calculated Standard Deviation from udf:", std_udf)
        print("\nManually Calculated Standard Deviation:", std_dev_manual)
        print(f"\nThese results sampled on {rows_to_generate} rows may be slightly different due to floating-point precision, but they should be very close")

if __name__ == "__main__":
    print("\nWorking on this code")
    main()

