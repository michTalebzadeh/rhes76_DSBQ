from pyspark.sql import SparkSession
import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import round
from src.config import ctest, test_url
from src.PrepTest import readSourceData, transformData, saveData, readSavedData

@pytest.fixture(scope = "session")
def initParameters():
    read_df = readSourceData()
    transform_df = transformData()

    spark_session = SparkSession.builder \
        .master('local[1]') \
        .appName(ctest['common']['appName']) \
        .getOrCreate()
    wSpecY = Window().partitionBy(F.date_format('datetaken', "yyyy"), 'regionname')
    return [spark_session, wSpecY]

def test_readMysqlData(option,initParameters):
    # read
    if option == 1:  ## input table
        table = ctest['statics']['dbschema']+'.'+ ctest['statics']['sourceTable']
        rows_to_check = ctest['statics']['read_df_rows']
    elif option == 2: ## output table
        table = ctest['statics']['dbschema']+'.'+ ctest['statics']['yearlyAveragePricesAllTable']
        rows_to_check = ctest['statics']['transformation_df_rows']
    spark_session = initParameters[0]
    # Read the test table
    read_df = spark_session.read. \
        format("jdbc"). \
        option("url", test_url). \
        option("driver", ctest['statics']['driver']). \
        option("dbtable", table). \
        option("user", ctest['statics']['user']). \
        option("password", ctest['statics']['password']). \
         load()
    assert read_df.count() == rows_to_check
    return read_df

def test_transformOracledata(initParameters):
    # 2) extract
    wSpecY = initParameters[1]
    read_df = test_readOracleData(1,initParameters)
    transformation_df = read_df. \
        select( \
        F.date_format('datetaken', 'yyyy').cast("Integer").alias('YEAR') \
        , 'REGIONNAME' \
        , round(F.avg('averageprice').over(wSpecY)).alias('AVGPRICEPERYEAR') \
        , round(F.avg('flatprice').over(wSpecY)).alias('AVGFLATPRICEPERYEAR') \
        , round(F.avg('TerracedPrice').over(wSpecY)).alias('AVGTERRACEDPRICEPERYEAR') \
        , round(F.avg('SemiDetachedPrice').over(wSpecY)).alias('AVGSDPRICEPRICEPERYEAR') \
        , round(F.avg('DetachedPrice').over(wSpecY)).alias('AVGDETACHEDPRICEPERYEAR')). \
        distinct().orderBy('datetaken', asending=True)
    assert transformation_df.count() == ctest['statics']['transformation_df_rows']
    return transformation_df

def test_extractOracleData(initParameters):
    # Write to test target table
    transformation_df = test_transformOracledata(initParameters)
    transformation_df. \
        write. \
        format("jdbc"). \
        option("url", test_url). \
        option("driver", ctest['statics']['driver']). \
        option("dbtable", ctest['statics']['dbschema']+'.'+ ctest['statics']['yearlyAveragePricesAllTable']). \
        option("user", ctest['statics']['user']). \
        option("password", ctest['statics']['password']). \
        mode(ctest['statics']['mode']). \
        save()
    # Check what is written is correct by reading it back from Oracle
    read_df = test_readOracleData(2,initParameters)
    assert transformation_df.subtract(read_df).count() == 0