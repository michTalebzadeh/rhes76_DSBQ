# Import necessary modules and libraries
import sys
#sys.path.append('/home/hduser/dba/bin/python/DSBQ/')
#sys.path.append('/home/hduser/dba/bin/python/DSBQ/conf')
#sys.path.append('/home/hduser/dba/bin/python/DSBQ/othermisc')
#sys.path.append('/home/hduser/dba/bin/python/DSBQ/src')
#sys.path.append('/home/hduser/dba/bin/python/DSBQ/tests')

import pytest
import math
from pyspark.sql import SparkSession
from pyspark import SparkContext
from src.spark_standard_deviation import SparkStandardDeviationCalculator

# This fixture sets up the Spark session and context for testing
@pytest.fixture(scope="module")
def spark_calculator():
    # Create a Spark session and context for testing
    spark_session = SparkSession.builder.appName("TestApp").getOrCreate()
    spark_context = SparkContext.getOrCreate()
    
    # Create an instance of the SparkStandardDeviationCalculator class
    calculator = SparkStandardDeviationCalculator.get_instance(spark_session, spark_context)
    
    # Yield the calculator instance for use in tests
    yield calculator

# Define test cases

# Test case to verify data generation
def test_generate_data(spark_calculator):
    # Define the number of rows to generate and the starting ID
    rows_to_generate = 100
    start = 1
    
    # Generate data using the calculator instance
    df = spark_calculator.generate_data(rows_to_generate, start)
    
    # Check if the number of rows generated matches the expected count
    assert df.count() == rows_to_generate

# Test case to calculate standard deviation
def test_calculate_stddev(spark_calculator):
    # Define a sample data set
    data = [1.0, 2.0, 3.0, 4.0, 5.0]
    
    # Calculate the standard deviation using the calculator instance
    stddev = spark_calculator.calculate_stddev(data)
    
    # Define the expected standard deviation (previously calculated)
    expected_stddev = 1.4142135623730951
    
    # Check if the calculated standard deviation is close to the expected value
    # Using math.isclose with a relative and absolute tolerance
    assert math.isclose(stddev, expected_stddev, rel_tol=1e-2, abs_tol=1e-2)

# Test case to check standard deviation calculation in Spark
def test_standard_deviation_calculation(spark_calculator):
    # Define the number of rows to generate and the starting ID
    rows_to_generate = 100
    start = 1
    
    # Generate data using the calculator instance
    df = spark_calculator.generate_data(rows_to_generate, start)

    # Calculate standard deviation using different methods
    std_dev_pyspark, std_udf, std_dev_manual = spark_calculator.calculate_standard_deviation(df)

    # Use the manually calculated standard deviation as the expected value
    expected_stddev = round(std_dev_manual, 2)
    
    # Check if the calculated standard deviations are close to the expected value
    # Using math.isclose with a relative tolerance
    assert math.isclose(std_dev_pyspark, expected_stddev, rel_tol=1e-2)
    assert math.isclose(std_udf, expected_stddev, rel_tol=1e-2)

# Run the tests if the script is executed directly
if __name__ == "__main__":
    pytest.main(["-s", __file__])
    # pytest test_spark_standard_deviation.py   
