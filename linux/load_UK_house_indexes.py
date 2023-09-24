#! /usr/bin/env python3
from __future__ import print_function
import sys
import findspark
findspark.init()
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext

def spark_session(appName):
  return SparkSession.builder \
        .appName(appName) \
        .enableHiveSupport() \
        .getOrCreate()


def sparkcontext():
  return SparkContext.getOrCreate()

def hivecontext():
  return HiveContext(sparkcontext())


appName = "DS"
spark =spark_session(appName)
##
## Get a DF first
##
csvlocation="hdfs://rhes75:9000/ds/UK-HPI-full-file-2020-01.csv"

rows = spark.read.csv(csvlocation, header="true").count()
print("\nnumber of rows is ",rows)
if (rows == 0):
         println("Empty CSV directory, aborting!")
         sys.exit(1)

house_df = spark.read.csv(csvlocation, header="true")
house_df.printSchema

# Map the columns to correct data types
##
for col_name in house_df.columns:
    if(col_name == "Date"):
        #house_df = house_df.withColumn(col_name, F.col(col_name).cast("date"))
        house_df = house_df.withColumn(col_name, F.col(col_name).cast("string"))
    elif(col_name == "Date"or col_name == "RegionName" or col_name == "AreaCode"):
        house_df = house_df.withColumn(col_name, F.col(col_name).cast("string"))
    else:
        house_df = house_df.withColumn(col_name, F.col(col_name).cast("double"))
house_df.printSchema()
house_df.createOrReplaceTempView("tmp")

## Check if table exist otherwise create it
DB = "DS"
tableName = "ukhouseprices"
fullyQualifiedTableName = DB + '.' + tableName
regionname = "Kensington and Chelsea"
spark.sql(f"""DROP TABLE IF EXISTS {fullyQualifiedTableName}_staging""")
sqltext = f"""
    CREATE TABLE {fullyQualifiedTableName}_staging(
         Datetaken  string
       , RegionName  string
       , AreaCode  string
       , AveragePrice  double
       , Index  double
       , IndexSA  double
       , oneMonthPercentChange  double
       , twelveMonthPercentChange  double
       , AveragePriceSA  double
       , SalesVolume  double
       , DetachedPrice  double
       , DetachedIndex  double
       , Detached1mPercentChange  double
       , Detached12mPercentChange  double
       , SemiDetachedPrice  double
       , SemiDetachedIndex  double
       , SemiDetached1mPercentChange  double
       , SemiDetached12mPercentChange  double
       , TerracedPrice  double
       , TerracedIndex  double
       , Terraced1mPercentChange  double
       , Terraced12mPercentChange  double
       , FlatPrice  double
       , FlatIndex  double
       , Flat1mPercentChange  double
       , Flat12mPercentChange  double
       , CashPrice  double
       , CashIndex  double
       , Cash1mPercentChange  double
       , Cash12mPercentChange  double
       , MortgagePrice  double
       , MortgageIndex  double
       , Mortgage1mPercentChange  double
       , Mortgage12mPercentChange  double
       , FTBPrice  double
       , FTBIndex  double
       , FTB1mPercentChange  double
       , FTB12mPercentChange  double
       , FOOPrice  double
       , FOOIndex  double
       , FOO1mPercentChange  double
       , FOO12mPercentChange  double
       , NewPrice  double
       , NewIndex  double
       , New1mPercentChange  double
       , New12mPercentChange  double
       , OldPrice  double
       , OldIndex  double
       , Old1mPercentChange  double
       , Old12mPercentChange  double
    )
"""
spark.sql(sqltext)
sqltext = f"""
          INSERT INTO {fullyQualifiedTableName}_staging
          SELECT
            --TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(date,'dd/MM/yyyy'),'yyyy-MM-dd')) AS datetaken
            date AS datetaken
          , RegionName
          , AreaCode
          , AveragePrice
          , Index
          , IndexSA
          , "1m%Change"
          , "12m%Change"
          , AveragePriceSA
          , SalesVolume
          , DetachedPrice
          , DetachedIndex
          , "Detached1m%Change"
          , "Detached12m%Change"
          , SemiDetachedPrice
          , SemiDetachedIndex
          , "SemiDetached1m%Change"
          , "SemiDetached12m%Change"
          , TerracedPrice
          , TerracedIndex
          , "Terraced1m%Change"
          , "Terraced12m%Change"
          , FlatPrice
          , FlatIndex
          , "Flat1m%Change"
          , "Flat12m%Change"
          , CashPrice
          , CashIndex
          , "Cash1m%Change"
          , "Cash12m%Change"
          , MortgagePrice
          , MortgageIndex
          , "Mortgage1m%Change"
          , "Mortgage12m%Change"
          , FTBPrice
          , FTBIndex
          , "FTB1m%Change"
          , "FTB12m%Change"
          , FOOPrice
          , FOOIndex
          , "FOO1m%Change"
          , "FOO12m%Change"
          , NewPrice
          , NewIndex
          , "New1m%Change"
          , "New12m%Change"
          , OldPrice
          , OldIndex
          , "Old1m%Change"
          , "Old12m%Change"
          FROM tmp
        """
spark.sql(sqltext)
# now icreate and populate the main table
spark.sql(f"""DROP TABLE IF EXISTS {fullyQualifiedTableName}""")
sqltext = f"""
    CREATE TABLE {fullyQualifiedTableName}(
         Datetaken  DATE COMMENT 'The year and month to which the monthly statistics apply'
       , RegionName  string COMMENT 'Name of geography (Country, Regional, County/Unitary/District Authority and London Borough)'
       , AreaCode  string COMMENT 'Code of geography (Country, Regional, County/Unitary/District Authority and London Borough)'
       , AveragePrice  double COMMENT 'Average house price for a geography in a particular period'
       , Index  double COMMENT 'House price index for a geography in a particular period (January 2015=100).'
       , IndexSA  double COMMENT 'Seasonally adjusted house price for a geography in a particular period (January 2015=100).'
       , oneMonthPercentChange  double COMMENT 'The percentage change in the Average Price compared to the previous month'
       , twelveMonthPercentChange  double COMMENT 'The percentage change in the Average Price compared to the same period twelve months earlier.'
       , AveragePriceSA  double COMMENT 'Seasonally adjusted Average Price for a geography in a particular period'
       , SalesVolume  double COMMENT 'Number of registered transactions for a geography in a particular period'
       , DetachedPrice  double COMMENT 'Average house price for a particular property type (such as detached houses), for a geography in a particular period.'
       , DetachedIndex  double COMMENT 'House price index for a particular property type (such as detached houses), for a geography in a particular period (January 2015=100).'
       , Detached1mPercentChange  double COMMENT 'The percentage change in the [Property Type Price (such as detached houses) compared to the previous month'
       , Detached12mPercentChange  double COMMENT 'The percentage change in the [Property Type Price (such as detached houses) compared to the same period twelve months earlier.'
       , SemiDetachedPrice  double
       , SemiDetachedIndex  double
       , SemiDetached1mPercentChange  double
       , SemiDetached12mPercentChange  double
       , TerracedPrice  double
       , TerracedIndex  double
       , Terraced1mPercentChange  double
       , Terraced12mPercentChange  double
       , FlatPrice  double
       , FlatIndex  double
       , Flat1mPercentChange  double
       , Flat12mPercentChange  double
       , CashPrice  double COMMENT 'Average house price by funding status (such as cash), for a geography in a particular period.'
       , CashIndex  double COMMENT 'House price index by funding status (such as cash), for a geography in a particular period (January 2015=100).'
       , Cash1mPercentChange  double
       , Cash12mPercentChange  double
       , MortgagePrice  double COMMENT 'Average house price by funding status (such as cash), for a geography in a particular period.'
       , MortgageIndex  double COMMENT 'House price index by funding status (such as cash), for a geography in a particular period (January 2015=100).'
       , Mortgage1mPercentChange  double
       , Mortgage12mPercentChange  double
       , FTBPrice  double COMMENT 'Average house price by buyer status (such as first time buyer/former owner occupier), for a geography in a particular period.'
       , FTBIndex  double COMMENT 'House price index by buyer status (such as first time buyer/former owner occupier), for a geography in a particular period. (January 2015=100).'
       , FTB1mPercentChange  double
       , FTB12mPercentChange  double
       , FOOPrice  double COMMENT 'Average house price by buyer status (such as first time buyer/former owner occupier), for a geography in a particular period.'
       , FOOIndex  double COMMENT 'House price index by buyer status (such as first time buyer/former owner occupier), for a geography in a particular period. (January 2015=100).'
       , FOO1mPercentChange  double
       , FOO12mPercentChange  double
       , NewPrice  double COMMENT 'Average house price by property status (such as new or existing property), for a geography in a particular period.'
       , NewIndex  double COMMENT 'House price index by property status (such as new or existing property), for a geography in a particular period. (January 2015=100).'
       , New1mPercentChange  double
       , New12mPercentChange  double
       , OldPrice  double  COMMENT 'Average house price by property status (such as new or existing property), for a geography in a particular period.'
       , OldIndex  double COMMENT 'House price index by property status (such as new or existing property), for a geography in a particular period. (January 2015=100).'
       , Old1mPercentChange  double
       , Old12mPercentChange  double
    )
    COMMENT 'from csv file UK-HPI-full-file-2020-01.csv through the staging table'
    STORED AS PARQUET
    TBLPROPERTIES ( "parquet.compress"="ZLIB" )
"""
spark.sql(sqltext)
sqltext = f"""
          INSERT INTO {fullyQualifiedTableName}
          SELECT
            TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(datetaken,'dd/MM/yyyy'),'yyyy-MM-dd')) AS datetaken
          , RegionName
          , AreaCode
          , AveragePrice
          , Index
          , IndexSA
          , oneMonthPercentChange
          , twelveMonthPercentChange
          , AveragePriceSA
          , SalesVolume
          , DetachedPrice
          , DetachedIndex
          , Detached1mPercentChange
          , Detached12mPercentChange
          , SemiDetachedPrice
          , SemiDetachedIndex
          , SemiDetached1mPercentChange
          , SemiDetached12mPercentChange
          , TerracedPrice
          , TerracedIndex
          , Terraced1mPercentChange
          , Terraced12mPercentChange
          , FlatPrice
          , FlatIndex
          , Flat1mPercentChange
          , Flat12mPercentChange
          , CashPrice
          , CashIndex
          , Cash1mPercentChange
          , Cash12mPercentChange
          , MortgagePrice
          , MortgageIndex
          , Mortgage1mPercentChange
          , Mortgage12mPercentChange
          , FTBPrice
          , FTBIndex
          , FTB1mPercentChange
          , FTB12mPercentChange
          , FOOPrice
          , FOOIndex
          , FOO1mPercentChange
          , FOO12mPercentChange
          , NewPrice
          , NewIndex
          , New1mPercentChange
          , New12mPercentChange
          , OldPrice
          , OldIndex
          , Old1mPercentChange
          , Old12mPercentChange
          FROM {fullyQualifiedTableName}_staging
        """
spark.sql(sqltext)
spark.sql(f"""DROP TABLE IF EXISTS {fullyQualifiedTableName}_staging""")
rows = spark.sql(f"""SELECT COUNT(1) FROM {fullyQualifiedTableName}""").collect()[0][0]
print("number of rows is ",rows)
spark.sql(f"""DROP TABLE IF EXISTS {DB}.summary""")
sqltext = f"""
   CREATE TABLE {DB}.summary
    COMMENT 'summary table with non null columns'
    STORED AS PARQUET
    TBLPROPERTIES ( "parquet.compress"="ZLIB" )
   AS
   SELECT
          datetaken
        , regionname
        , areacode
        , averageprice
        , index
        , salesvolume
        , detachedprice
        , detachedindex
        , semidetachedprice
        , semidetachedindex
        , terracedprice
        , terracedindex
        , flatprice
        , flatindex
        , cashprice
        , cashindex
        , mortgageprice
        , mortgageindex
        , ftbprice
        , ftbindex
        , fooprice
        , fooindex
        , newindex
        , oldprice
        , oldindex
    FROM  {fullyQualifiedTableName}
    WHERE regionname = '{regionname}'
"""
spark.sql(sqltext)
rows = spark.sql(f"""SELECT COUNT(1) FROM {DB}.summary""").collect()[0][0]
print("number of rows in summary table is ",rows)
sys.exit()
