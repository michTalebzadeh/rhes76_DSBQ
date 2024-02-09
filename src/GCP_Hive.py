from __future__ import print_function
from config import config, hive_url
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
from sparkutils import sparkstuff as s
from othermisc import usedFunctions as uf
import locale
locale.setlocale(locale.LC_ALL, 'en_GB')


class GCP_Hive:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.config = config

    def extractHiveData(self):

        print (f"""Getting average yearly prices per region for all""")
        # read data through jdbc from Hive
        wSpecY = Window().partitionBy(F.date_format('datetaken', "yyyy"), 'regionname')
        tableName=self.config['GCPVariables']['sourceTable']
        fullyQualifiedTableName = self.config['hiveVariables']['DSDB']+'.'+tableName
        user = self.config['hiveVariables']['hive_user']
        password = self.config['hiveVariables']['hive_password']
        driver = self.config['hiveVariables']['hive_driver']
        fetchsize = self.config['hiveVariables']['fetchsize']
        print("reading from Hive table")
        house_df = s.loadTableFromJDBC(self.spark,hive_url,fullyQualifiedTableName,user,password,driver,fetchsize)
        house_df.printSchema()
        house_df.show(5,False)
        return house_df

    def transformHiveData(self, house_df):

        print(f"""\nAnnual House prices per regions in GBP""")
        # Workout yearly aversge prices
        wSpecY = Window().partitionBy(F.date_format('datetaken', "yyyy"), 'regionname')
        df2 = house_df. \
                        select( \
                              F.date_format('datetaken', 'yyyy').cast("Integer").alias('year') \
                            , 'regionname' \
                            , round(F.avg('averageprice').over(wSpecY)).alias('AVGPricePerYear') \
                            , round(F.avg('flatprice').over(wSpecY)).alias('AVGFlatPricePerYear') \
                            , round(F.avg('TerracedPrice').over(wSpecY)).alias('AVGTerracedPricePerYear') \
                            , round(F.avg('SemiDetachedPrice').over(wSpecY)).alias('AVGSemiDetachedPricePerYear') \
                            , round(F.avg('DetachedPrice').over(wSpecY)).alias('AVGDetachedPricePerYear')). \
                        distinct().orderBy('datetaken', asending=True)
        df2.show(20,False)
        return df2

    def loadIntoBQTable(self, df2):
        # write to BigQuery table
        s.writeTableToBQ(df2,"overwrite",config['GCPVariables']['targetDataset'],config['GCPVariables']['yearlyAveragePricesAllTable'])
        print(f"""created {config['GCPVariables']['yearlyAveragePricesAllTable']}""")
        # read data to ensure all loaded OK
        read_df = s.loadTableFromBQ(self.spark, config['GCPVariables']['targetDataset'], config['GCPVariables']['yearlyAveragePricesAllTable'])
        # check that all rows are there
        if df2.subtract(read_df).count() == 0:
            print("Data has been loaded OK to BQ table")
        else:
            print("Data could not be loaded to BQ table, quitting")
            sys.exit(1)

if __name__ == "__main__":
    appName = config['common']['appName']
    spark_session = s.spark_session(appName)
    spark_session = s.setSparkConfHive(spark_session)
    spark_session = s.setSparkConfBQ(spark_session)
    lst = (spark_session.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");uf.println(lst)
    gcphive = GCP_Hive(spark_session)
    house_df = gcphive.extractHiveData()
    df2 = gcphive.transformHiveData(house_df)
    gcphive.loadIntoBQTable(df2)
    lst = (spark_session.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at");uf.println(lst)

