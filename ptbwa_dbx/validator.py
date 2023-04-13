from abc import ABC, abstractmethod
from functools import cached_property
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date

# https://ptbwa.atlassian.net/wiki/spaces/DS/pages/120356974/Filtering+Ruleset

class SparkBuilder:

    def __init__(self, app_name=None) -> None:
        self._app_name = app_name

    @cached_property
    def spark(self):
        if not self._app_name:
            raise ValueError("[REQUIRED]APP_NAME")
        return SparkSession.builder.appName(self._app_name).getOrCreate()

# (!) load cached spark
# spark = SparkBuilder(app_name="fraud_validator").spark

class FraudValidator:
    """
    A class as root for validating lots of fraud data

    """

    _valid_types = [
        "device",
        "app",
        "viewability",
        "device",
    ]

    def _create(cls, valid_type):


        return object.__new__(f"{type}")

    @abstractmethod
    def is_valid(self):
        pass

class ViewabilityValidator(FraudValidator):
    """
    A class for validating viewability fraud data

    """

    def __init__(self) -> None:
        super().__init__()

    def is_valid(self):
        pass


class DeviceValidator(FraudValidator):
    """
    A class for validating device fraud data

    """

    def __init__(self) -> None:
        super().__init__()

    def is_valid(self):
        pass

class CombineValidator(FraudValidator):
    """
    A class for validating combined fraud data

    """

    def __init__(self) -> None:
        super().__init__()

    def is_valid(self):
        pass

class CountHourly(FraudValidator):

    def __init__(self) -> None:
        super().__init__()

    def is_valid(self):
        pass




