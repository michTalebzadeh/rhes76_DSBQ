import pytest
import yaml

@pytest.fixture(scope = "session",autouse=True)
def validateConfigValues():
    with open("../conf/config.yml", 'r') as file:
        config: dict = yaml.load(file.read(), Loader=yaml.FullLoader)
        tableName = config['GCPVariables']['sourceTable']
    return tableName