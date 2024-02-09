import pytest

@pytest.fixture(scope = "session")
def readSavedData():
    # read target table to tally the result
    table = ctest['statics']['dbschema'] + '.' + ctest['statics']['yearlyAveragePricesAllTable']
    spark_session = s.spark_session(ctest['common']['appName'])
    try:
        readSavedData_df = spark_session.read. \
            format("jdbc"). \
            option("url", test_url). \
            option("driver", ctest['statics']['driver']). \
            option("dbtable", table). \
            option("user", ctest['statics']['user']). \
            option("password", ctest['statics']['password']). \
            load()
        return readSavedData_df
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)