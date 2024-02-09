import pytest
import yaml

@pytest.fixture(scope = "session",autouse=True)
def validateURL():
    with open("../conf/config.yml", 'r') as file:
        config: dict = yaml.load(file.read(), Loader=yaml.FullLoader)
        hive_url = "jdbc:hive2://" + config['hiveVariables']['hiveHost'] + ':' + config['hiveVariables']['hivePort'] + '/default'
    return hive_url