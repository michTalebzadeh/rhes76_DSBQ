import pytest
import sys
from src.config import ctest, test_url

@pytest.fixture(scope = "session",autouse=True)
def loadIntoMysqlTable(extractHiveData):
    try:
        extractHiveData. \
            write. \
            format("jdbc"). \
            option("url", test_url). \
            option("dbtable", ctest['statics']['sourceTable']). \
            option("user", ctest['statics']['user']). \
            option("password", ctest['statics']['password']). \
            option("driver", ctest['statics']['driver']). \
            mode(ctest['statics']['mode']). \
            save()
        return True
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)
