from __future__ import print_function
import sys
from DSBQ.src.configure import config, oracle_url
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round, current_timestamp, lit
from pyspark.sql.window import Window
from DSBQ.sparkutils import sparkstuff as s
from DSBQ.othermisc import usedFunctions as uf
#import locale
#locale.setlocale(locale.LC_ALL, 'en_GB')
import cx_Oracle
import datetime

def main():
    appName = "RandomDataBigQuery"
    spark_session = s.spark_session(appName)
    spark_session = s.setSparkConfBQ(spark_session)
    spark_context = s.sparkcontext()
    spark_context.setLogLevel("ERROR")
    lst = (spark_session.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");uf.println(lst)
    randomdatabq = RandomData(spark_session, spark_context)
    dfRandom = randomdatabq.generateRamdomData()
    dfRandom.printSchema()
    dfRandom.show(20, False)
    randomdatabq.loadIntoBQTable(dfRandom)
    lst = (spark_session.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at");uf.println(lst)
    spark_session.stop()

class RandomData:
    def __init__(self, spark_session, spark_context):
        self.spark = spark_session
        self.sc = spark_context
        self.config = config

    def getMaxIDFromBQTable(self):
        maxID = 0
        dataset = "test"
        tableName = "randomData"
        fullyQualifiedTableName = dataset+'.'+tableName
        #read_df = s.loadTableFromBQ(self.spark, dataset, tableName)
        #read_df.printSchema()
        #read_df.show(20,False)
        #maxID = read_df.agg({"ID": "max"}).collect()[0][0]
        return maxID

    def generateRamdomData(self):
        numRows = 100
        rows = 0
        maxID = self.getMaxIDFromBQTable()
        start = 0
        if (rows == 0):
          start = 1
        else:
          start = maxID + 1
        end = start + numRows
        print("starting at ID = ", start, ",ending on = ", end)
        Range = range(start, end)
        ## This traverses through the Range and increment "x" by one unit each time, and that x value is used in the code to generate random data through Python functions in a class
        rdd = self.sc.parallelize(Range). \
            map(lambda x: (x, uf.clustered(x, numRows), \
                           uf.scattered(x, numRows), \
                           uf.randomised(x, numRows), \
                           uf.randomString(50), \
                           uf.padString(x, " ", 50),
                           uf.padSingleChar("x", 50)))
        df = rdd.toDF(). \
            withColumnRenamed("_1", "ID"). \
            withColumnRenamed("_2", "CLUSTERED"). \
            withColumnRenamed("_3", "SCATTERED"). \
            withColumnRenamed("_4", "RANDOMISED"). \
            withColumnRenamed("_5", "RANDOM_STRING"). \
            withColumnRenamed("_6", "SMALL_VC"). \
            withColumnRenamed("_7", "PADDING"). \
            withColumn("op_type", lit(config['MDVariables']['op_type'])). \
            withColumn("op_time", current_timestamp())
        return df

    def loadIntoBQTable(self, df2):
        # write to BigQuery table
        dataset = "test"
        tableName = "randomData"
        fullyQualifiedTableName = dataset+'.'+tableName
        s.writeTableToBQ(df2,"append",dataset,tableName)
        print(f"""\n Populated BigQuery table {fullyQualifiedTableName}""")
        print("\n rows written is ",  df2.count())
        print(f"""\n Reading from BigQuery table {fullyQualifiedTableName}\n""")
        # read data to ensure all loaded OK
        read_df = s.loadTableFromBQ(self.spark, dataset, tableName)
        print("\n rows read in is ",  read_df.count())

if __name__ == "__main__":
  main()
