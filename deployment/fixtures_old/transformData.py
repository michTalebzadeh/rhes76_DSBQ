import pytest
@pytest.fixture
def transformData():
    # 2) extract
    read_df = readSourceData()
    wSpecY = Window().partitionBy(F.date_format('datetaken', "yyyy"), 'regionname')
    try:
        transformation_df = read_df. \
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