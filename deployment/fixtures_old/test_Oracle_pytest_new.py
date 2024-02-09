from pyspark.sql import SparkSession
import pytest
from sparkutils import sparkstuff as s
from src.config import ctest, test_url
from src.CreateSampleDataInMysql import extractHiveData, loadIntoMysqlTable, readSourceData, transformData, saveData, readSavedData

"""
@pytest.fixtures_old(scope = "session")
def initParameters():
    # Prepare test data here in this fixtures_old
    appName = ctest['common']['appName']
    spark_session = s.spark_session(appName)
    # create sample data
    # read Hive source table and select read_df number of rows (see config_test.yml)
    house_df = extractHiveData()  ## read Hive table as sample source
    # write to Mysql DB
    loadIntoMysqlTable(house_df)
    # data is ready to be tested in mysql
    read_df = readSourceData()
    # do Transform part of ETL (Extract, Transform, Load)
    transformation_df = transformData()
    # save data to target test table in mysql
    saveData()
    # read that data saved to ensure that the rows will tally
    readSavedData_df = readSavedData()
    return [read_df, transformation_df, readSavedData_df]
"""
def test_validity():
    house_df = extractHiveData()
    loadIntoMysqlTable(house_df)
    # Assert that data read from source table is what is expected
    read_df = readSourceData()
    assert read_df.count() == ctest['statics']['read_df_rows']
    # Assert data written to target table is what it should be
    transformation_df = transformData()
    assert transformation_df.count() == ctest['statics']['transformation_df_rows']
    # Assert what is written tallies with the number of rows transformed
    readSavedData_df = readSavedData()
    assert readSavedData_df.subtract(transformation_df).count() == 0