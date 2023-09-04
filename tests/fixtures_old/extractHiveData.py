import pytest
from src.config import config, hive_url
from src.config import ctest, test_url
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from sparkutils import sparkstuff as s

@pytest.fixture(scope = "session",autouse=True)
def extractHiveData():
    print(f"""Getting average yearly prices per region for all""")
    # read data through jdbc from Hive
    spark_session = s.spark_session(ctest['common']['appName'])
    tableName = config['GCPVariables']['sourceTable']
    fullyQualifiedTableName = config['hiveVariables']['DSDB'] + '.' + tableName
    user = config['hiveVariables']['hive_user']
    password = config['hiveVariables']['hive_password']
    driver = config['hiveVariables']['hive_driver']
    fetchsize = config['hiveVariables']['fetchsize']
    print("reading from Hive table")
    house_df = s.loadTableFromJDBC(spark_session,hive_url,fullyQualifiedTableName,user,password,driver,fetchsize)
    # sample data equally n rows from Kensington and Chelsea and n rows from City of Westminster
    num_rows = int(ctest['statics']['read_df_rows']/2)
    house_df = house_df.filter(col("regionname") == "Kensington and Chelsea").limit(num_rows).unionAll(house_df.filter(col("regionname") == "City of Westminster").limit(num_rows))
    return house_df
