import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import IntegerType

def avg_number(a,b):
    try:
       avg_number=a+b
       return avg_number
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)

def main():
    appName = "avg_number"
    # Initialize a Spark session
    spark = SparkSession.builder.appName(appName).getOrCreate()

    # Register UDF
    avg_number_udf = udf(avg_number, IntegerType())

    a=2
    b=3 
   # Create literal values using lit() and use them in the UDF
    a_lit = lit(a)
    b_lit = lit(b)

    # You need to use the UDF in a DataFrame operation
    df = spark.range(1).select(avg_number_udf(a_lit, b_lit).alias("result"))
    df.show()

if __name__ == "__main__":
    main()
