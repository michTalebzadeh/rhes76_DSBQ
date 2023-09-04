import pytest
import sys
from src.config import ctest, test_url

@pytest.fixture(scope = "session",autouse=True)
def saveData(transformData):
    # Write to test target table
    try:
        transformData. \
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
        return True
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)
