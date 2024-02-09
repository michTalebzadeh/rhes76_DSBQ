from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Create a Spark session
spark = SparkSession.builder.appName("SubstringUDFExample").getOrCreate()

# Sample DataFrame
df = spark.createDataFrame([(1, "Spark"), (2, "Python"), (3, "Data")], ["id", "word"])

# Define a Python function to extract a substring
def substring_word(word, start_position, end_position):
    # Using Python string slicing to extract the substring
    return word[start_position - 1:end_position]

# Create a UDF from the Python function
substring_udf = udf(substring_word, StringType())

word="Is a big world"
print(substring_udf, word, 1,5)

# Add a new column using the UDF
df = df.withColumn("substring_result", substring_udf(df["word"], 2, 4))

# Show the DataFrame with the new column
df.show(truncate=False)

# Stop the Spark session
spark.stop()


