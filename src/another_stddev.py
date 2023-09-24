from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop

spark = SparkSession.builder.getOrCreate()

data = [(52.7,), (45.3,), (60.2,), (53.8,), (49.1,), (44.6,), (58.0,), (56.5,), (47.9,), (50.3,)]
df = spark.createDataFrame(data, ["value"])

df.select(stddev_samp("value").alias("Sample Standard Deviation") \
       , (stddev_pop("value").alias("Population Standard Deviation"))).show()


