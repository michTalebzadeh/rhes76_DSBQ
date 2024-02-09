from __future__ import print_function
import sys
import json
sys.path.append('/home/hduser/dba/bin/python/DSBQ/')
sys.path.append('/home/hduser/dba/bin/python/DSBQ/conf')
sys.path.append('/home/hduser/dba/bin/python/DSBQ/othermisc')
sys.path.append('/home/hduser/dba/bin/python/DSBQ/src')
import findspark
findspark.init()
from config import config
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round, current_timestamp, lit
from pyspark.sql.types import StructType, StringType,IntegerType, FloatType, TimestampType
from pyspark.sql.window import Window
from sparkutils import sparkstuff as s
from othermisc import usedFunctions as uf
import datetime
import uuid

def main():
    appName = "RandomDataUUID"
    spark_session = s.spark_session(appName)
    spark_session = s.setSparkConfBQ(spark_session)
    spark_context = s.sparkcontext()
    # Set the log level to ERROR to reduce verbosity
    spark_context.setLogLevel("ERROR")
    lst = (spark_session.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");uf.println(lst)
    randomdatabq = RandomData(spark_session, spark_context)
    dfRandom = randomdatabq.generateRamdomData(appName)
    lst = (spark_session.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at");uf.println(lst)
    spark_session.stop()

class RandomData:
    def __init__(self, spark_session, spark_context):
        self.spark = spark_session
        self.sc = spark_context
        self.config = config

    def readDataFromBQTable(self):
        dataset = "test"
        tableName = "randomdata"
        fullyQualifiedTableName = dataset+'.'+tableName
        read_df = s.loadTableFromBQ(self.spark, dataset, tableName)
        return read_df

    def getValuesFromBQTable(self):
        read_df = self.readDataFromBQTable()
        read_df.createOrReplaceTempView("tmp_view")
        rows = self.spark.sql("SELECT COUNT(1) FROM tmp_view").collect()[0][0]
        maxID = self.spark.sql("SELECT MAX(ID) FROM tmp_view").collect()[0][0]
        return {"rows":rows,"maxID":maxID}
  
    def generateRamdomData(self, appName):
        numRows = 10
        rows = 0
        values = self.getValuesFromBQTable()
        rows = values["rows"]
        maxID = values["maxID"]
        start = 0
        if (rows == 0):
          start = 1
        else:
          start = maxID + 1
        end = start + numRows
        print("starting at ID = ", start, ",ending on = ", end)
        Range = range(start, end)
        # Kafka producer requires a key, value pair. We generate UUID key as the unique identifier of Kafka record
        ## This traverses through the Range and increment "x" by one unit each time, and that x value is used in the code to generate random data through Python functions in a class
        
        rdd = self.sc.parallelize(Range). \
            map(lambda x: (str(uuid.uuid4()), 
                          x, uf.clustered(x, numRows), \
                          uf.scattered(x, numRows),
                          uf.randomised(x, numRows), \
                          uf.randomString(50),
                          uf.padString(x, " ", 50), \
                          uf.padSingleChar("x", 50)))
        # Convert RDD to DataFrame
        df = rdd.toDF(["KEY", "ID", "CLUSTERED", "SCATTERED", "RANDOMISED", "RANDOM_STRING", "SMALL_VC", "PADDING"])

        # Add metadata columns
        df = df.withColumn("op_type", lit(config['MDVariables']['op_type'])). \
             withColumn("op_time", current_timestamp())
        # Create key-value map
        key_value_map = df.rdd.map(lambda row: (row["KEY"], row)).collectAsMap()

        # Convert datetime values to strings before saving to JSON
        key_value_map = df.rdd.map(lambda row: (row["KEY"], row)).collectAsMap()
        for key, value in key_value_map.items():
          if isinstance(value["op_time"], datetime.datetime):
            value = dict(zip(value.asDict().keys(), value))  # Create dictionary from Row
            value["op_time"] = value["op_time"].strftime("%Y-%m-%d %H:%M:%S")

        # Convert all datetime values to strings in the map
        key_value_map_str = {}
        for key, value in key_value_map.items():
          value_dict = value.asDict()
          key_value_map_str[key] = {k: v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime.datetime) else v
                              for k, v in value_dict.items()}

        # Save the map to JSON
        with open(f"""/home/hduser/dba/bin/python/DSBQ/src/{appName}.json""", "w") as file:
          json.dump(key_value_map_str, file, indent=4)
        
        print(f"""Key-value pairs saved to file {appName}!""")
        
        print (f"""Print schema and JSON representation (two rows only)\n""")
        df.printSchema()
        df_json = df.toJSON()
        for row in df_json.take(2):
          print(row)
       
        return df

if __name__ == "__main__":
  main()
