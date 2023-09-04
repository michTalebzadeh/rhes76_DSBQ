import pytest
from src.config import ctest
import pytest
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import HiveContext

@pytest.mark.usefixtures("spark")
def test_createDataFrame(spark):
    df1 = spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss')").collect()
    df2 = spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss')").collect()
    assert df2 > df1

@pytest.mark.usefixtures("validateConfigValues")
def test_validate_configs(validateConfigValues):
    assert validateConfigValues == "ukhouseprices"

@pytest.mark.usefixtures("validateURL")
def test_validate_URL(validateURL):
    assert validateURL == "jdbc:hive2://rhes75:10099/default"

@pytest.mark.usefixtures("extractHiveData")
def test_extract(extractHiveData):
    assert extractHiveData.count() == ctest['statics']['read_df_rows']

@pytest.mark.usefixtures("loadIntoMysqlTable")
def test_loadIntoMysqlTable(loadIntoMysqlTable):
    assert loadIntoMysqlTable

@pytest.mark.usefixtures("readSourceData")
def test_readSourceData(readSourceData):
    assert readSourceData.count() == ctest['statics']['read_df_rows']

@pytest.mark.usefixtures("transformData")
def test_transformData(transformData):
    assert transformData.count() == ctest['statics']['transformation_df_rows']

@pytest.mark.usefixtures("saveData")
def test_saveData(saveData):
    assert saveData

def test_readSavedData(transformData, readSavedData):
    assert readSavedData.subtract(transformData).count() == 0
