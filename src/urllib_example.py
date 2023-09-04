import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField

import urllib.request

def download_url_content(url):
    try:
        response = urllib.request.urlopen(url)
        return response.read().decode('utf-8')  # Assuming UTF-8 encoding
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)

def main():
    # Initialize a Spark session
    spark = SparkSession.builder.appName("URLDownload").getOrCreate()

    # Sample DataFrame with URLs
    data = [("https://www.bbc.co.uk",), ("https://www.bbc.com",)]
    
    # Define the schema for the DataFrame
    schema = StructType([StructField("url", StringType(), True)])

    df = spark.createDataFrame(data, schema=schema)

    # Register UDF to download content
    download_udf = udf(download_url_content, StringType())

    # Add a new column with downloaded content
    df_with_content = df.withColumn("content", download_udf(df["url"]))

    # Show the DataFrame with downloaded content
    df_with_content.show(truncate=False)

if __name__ == "__main__":
    main()
