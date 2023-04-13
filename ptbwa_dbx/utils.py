from typing import Any, Iterable, List, Optional, Tuple, Type, TypeVar, Union
from functools import cached_property
from logging import Logger

from pyspark.sql import SparkSession
from ptbwa_dbx.type import DataType


class SparkBuilder:
    """
    A class for building spark instance in cache

    """

    def __init__(self, app_name=None) -> None:
        self._app_name = app_name if app_name else DataType.DMP.value

    @cached_property
    def spark(self) -> SparkSession:
        return SparkSession.builder.appName(self._app_name).getOrCreate()

class Logger:
    """
    A class for logging in custom

    """

    def __init__(self, log_name: str = None, spark: SparkSession = None) -> None:
        self._log_name = log_name if log_name else DataType.DEFAULT_LOG_NAME.value
        self._spark = spark if spark else SparkBuilder(app_name=DataType.DMP.value).spark

    @cached_property
    def log(self) -> Logger:
        """
        A method for setting custom log

        :return:
        """
        log4jLogger = self._spark.sparkContext._jvm.org.apache.log4j
        return log4jLogger.LogManager.getLogger(self._log_name)





