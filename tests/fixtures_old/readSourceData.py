from __future__ import print_function
import pytest
import sys
from src.config import config, ctest
from src.config import ctest, test_url
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from sparkutils import sparkstuff as s
from pyspark.sql.window import Window
import locale
locale.setlocale(locale.LC_ALL, 'en_GB')
@pytest.fixture(scope = "session",autouse=True)
def readSourceData():
    # read source table
    table = ctest['statics']['dbschema'] + '.' + ctest['statics']['sourceTable']
    spark_session = s.spark_session(ctest['common']['appName'])
    # Read the test table
    try:
        read_df = spark_session.read. \
            format("jdbc"). \
            option("url", test_url). \
            option("driver", ctest['statics']['driver']). \
            option("dbtable", table). \
            option("user", ctest['statics']['user']). \
            option("password", ctest['statics']['password']). \
            option("fetchsize", ctest['statics']['fetchsize']). \
            load()
        return read_df
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)