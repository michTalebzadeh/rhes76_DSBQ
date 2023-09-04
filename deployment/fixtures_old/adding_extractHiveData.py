import pytest
@pytest.mark.usefixtures("extractHiveData")
def test_extractHiveData(extractHiveData):
    assert extractHiveData(house_df).count() > 0