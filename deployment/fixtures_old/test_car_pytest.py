import pytest
from src.Car import Car

"""
    The purpose of a test fixtures_old or sample data is to ensure that there is a well known and fixed environment in which tests are run so that results are repeatable.
    Some people call this the test context.
    Examples of fixtures_old:

    Loading a database with a specific, known set of data
    Erasing a hard disk and installing a known clean operating system installation
    Copying a specific known set of files
    Preparation of input data and set-up/creation of fake or mock objects
"""
speed_data = [45, 50, 55, 100]

@pytest.mark.parametrize("speed_brake", speed_data)
def test_car_brake(speed_brake):
    car = Car(50)
    car.brake()
    assert car.speed == speed_brake

@pytest.mark.parametrize("speed_accelerate", speed_data)
def test_car_accelerate(speed_accelerate):
    car = Car(50)
    car.accelerate()
    assert car.speed >= speed_accelerate
