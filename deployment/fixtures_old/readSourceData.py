import pytest
@pytest.fixture
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
            load()
        return read_df
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)