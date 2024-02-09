import pytest
@pytest.fixture(scope = "session")
def saveData():
    # Write to test target table
    transformation_df = transformData()
    try:
        transformation_df. \
            write. \
            format("jdbc"). \
            option("url", test_url). \
            option("driver", ctest['statics']['driver']). \
            option("dbtable",
                   ctest['statics']['dbschema'] + '.' + ctest['statics']['yearlyAveragePricesAllTable']). \
            option("user", ctest['statics']['user']). \
            option("password", ctest['statics']['password']). \
            mode(ctest['statics']['mode']). \
            save()
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)
