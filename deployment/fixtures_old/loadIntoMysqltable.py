import pytest

@pytest.fixture(scope = "session")
def loadIntoMysqlTable(house_df):
    # write to Mysql table
    s.writeTableToMysql(house_df,"overwrite",config['MysqlVariables']['dbschema'],config['MysqlVariables']['sourceTable'])
    print(f"""created {config['MysqlVariables']['sourceTable']}""")