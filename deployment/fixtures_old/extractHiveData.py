import pytest
import sys
from src.config import config
from src.config import ctest, test_url
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from sparkutils import sparkstuff as s
from pyspark.sql.window import Window
import locale
locale.setlocale(locale.LC_ALL, 'en_GB')
@pytest.fixture
def extractHiveData():
    print(f"""Getting average yearly prices per region for all""")
    # read data through jdbc from Hive
    spark_session = s.spark_session(ctest['common']['appName'])
    tableName = config['GCPVariables']['sourceTable']
    fullyQualifiedTableName = config['hiveVariables']['DSDB'] + '.' + tableName
    print("reading from Hive table")
    house_df = s.loadTableFromHiveJDBC(spark_session, fullyQualifiedTableName)
    # sample data equally n rows from Kensington and Chelsea and n rows from City of Westminster
    num_rows = int(config['MysqlVariables']['read_df_rows']/2)
    house_df = house_df.filter(col("regionname") == "Kensington and Chelsea").limit(num_rows).unionAll(house_df.filter(col("regionname") == "City of Westminster").limit(num_rows))
    #house_df.printSchema()
    #house_df.show(5, False)
    #return house_df