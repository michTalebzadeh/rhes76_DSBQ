from pyspark.sql import SparkSession
from pyspark.sql.functions import max, col
import os

spark = SparkSession.builder.appName("StudentScoreTransformation").getOrCreate()

# Load the main table into a DataFrame from a csv file on local
current_dir = os.getcwd()
input_file_path = f"file:///{current_dir}/main_table.csv"
main_table = spark.read.format("csv").option("header", "true").load(input_file_path)
print(f"output from source table\n")
main_table.show(100, False)
# Pivot the subjects into columns. In this case, the operation is applied to the "Subjects" column, and each unique subject 
# becomes a separate column in the pivoted result.

pivoted_table = main_table.groupBy("StudentID").pivot("Subjects").agg(max("Scores"))

# Rename the columns to remove spaces and make them more Python-friendly
column_names = ["StudentID"] + [f"{subject.replace(' ', '_')}_Score"
for subject in main_table.select("Subjects").distinct().rdd.flatMap(lambda x: x).collect()]
pivoted_table = pivoted_table.toDF(*column_names)

# Show the resulting DataFrame
print(f"output from pivoted table\n")
pivoted_table.show(truncate=False)

output_file_path = f"file:///{current_dir}/output_table.csv"
# Write the result to a CSV file or any other desired format
pivoted_table.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_file_path)

# Stop the Spark session
spark.stop()
