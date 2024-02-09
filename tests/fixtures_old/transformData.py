import pytest
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.window import Window

@pytest.fixture(scope = "session",autouse=True)
def transformData(readSourceData):
    wSpecY = Window().partitionBy(F.date_format('datetaken', "yyyy"), 'regionname')
    try:
        transformation_df = readSourceData. \
            select( \
            F.date_format('datetaken', 'yyyy').cast("Integer").alias('YEAR') \
            , 'REGIONNAME' \
            , round(F.avg('averageprice').over(wSpecY)).alias('AVGPRICEPERYEAR') \
            , round(F.avg('flatprice').over(wSpecY)).alias('AVGFLATPRICEPERYEAR') \
            , round(F.avg('TerracedPrice').over(wSpecY)).alias('AVGTERRACEDPRICEPERYEAR') \
            , round(F.avg('SemiDetachedPrice').over(wSpecY)).alias('AVGSDPRICEPRICEPERYEAR') \
            , round(F.avg('DetachedPrice').over(wSpecY)).alias('AVGDETACHEDPRICEPERYEAR')). \
            distinct().orderBy('datetaken', asending=True)
        return transformation_df
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)