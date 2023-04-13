from functools import cached_property

from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date

class SparkBuilder:

    def __init__(self, app_name=None) -> None:
        self._app_name = app_name

    @cached_property
    def create(self):
        if not self._app_name:
            raise ValueError("[REQUIRED]APP-NAME")
        return SparkSession.builder.appName(self._app_name).getOrCreate()

spark_builder = SparkBuilder(
    app_name="dbx"
)

spark = spark_builder.create

with open("/dbfs/FileStore/sql/sample.sql", 'r') as f:
    tmp = f.readlines()

query = f"""{''.join(tmp)}"""


df = spark.sql(query)
df.show()

# path = "dbfs:/mnt/ptbwa-basic/topics/streams_imp_app_nhn/year=2023/month=03/day=23/hour=11/minute=40/"
#
# df = spark.read.format("json").load(path)
# df.show(5)



