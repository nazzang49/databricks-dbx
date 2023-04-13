from enum import Enum

from pyspark.sql import SparkSession, dataframe
# from sparkdl.xgboost import *
from pyspark.ml.regression import *
from pyspark.ml.feature import VectorAssembler, VectorIndexer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import PipelineModel
from pyspark.ml import Pipeline

# from type import DataType

import os

class PropfitCommonType(Enum):
    """
    A class for using propfit types e.g. ml

    """
    PROPFIT = "propfit"
    DMP = "dmp"
    GA4 = "ga4"
    JOIN = "join"

spark = SparkSession.builder.appName("SYNTHETIC_MODELING").getOrCreate()

# https://dbc-024ee768-9ab2.cloud.databricks.com/?o=2776940675122529#notebook/2416970997867031/command/2416970997867036

class MyQuery:
    """
    A class for creating queries based on data types
    """

    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return ('Field('
                # f'table={self.table!r},'
                ')')

    @classmethod
    def get_query(cls, query_type: str, advertiser: str = None):
        try:
            query_type = getattr(PropfitCommonType, query_type.upper()).value
            base_path = os.path.join("/dbfs/FileStore/sql", advertiser if advertiser else "")
            file_path = os.path.join(base_path, f"{query_type}.sql")

            with open(file_path, 'r') as f:
                query = f.readlines()
            return f"""{''.join(query)}"""
        except Exception:
            raise ValueError(f"[NOT_FOUND_QUERY]DATA_TYPE::{query_type}")

class MyValidator:
    """
    A class for creating validator to check arguments
    """

    _REQUIRED_DATAFRAMES = [
        "df_propfit",
        "df_dmp",
        "df_ga4",
    ]

    _CANDIDATES_CASES = {
        "camel": [
            "snake"
        ],
        "snake": [
            "camel"
        ],
    }

    def __init__(self) -> None:
        pass

    def __repr__(self) -> str:
        return super().__repr__()

    @staticmethod
    def is_valid_dataframes(**kwargs):
        for dataframe in MyValidator._REQUIRED_DATAFRAMES:
            if dataframe not in kwargs:
                raise ValueError(f"[NOT_FOUND_{dataframe.upper()}]REQUIRED")

    @staticmethod
    def is_valid_cases(before: str, after: str):
        if before not in MyValidator._CANDIDATES_CASES:
            raise ValueError(f"[NOT_FOUND_{before.upper()}]BEFORE_CASE")

        if after not in MyValidator._CANDIDATES_CASES[before]:
            raise ValueError(f"[NOT_FOUND_{after.upper()}]AFTER_CASE")

class MyUtils:
    """
    A class for creating util functions
    """

    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return super().__repr__()

    @staticmethod
    def _convert_camel_to_snake(col: str):
        import re
        snaked_col = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', col)
        snaked_col = re.sub('([a-z0-9])([A-Z])', r'\1_\2', snaked_col)
        return snaked_col.lower()

    @staticmethod
    def convert_case(df: dataframe.DataFrame, before: str, after: str):
        MyValidator.is_valid_cases(before, after)

        for col in df.columns:
            case_converted_col = getattr(MyUtils, f"_convert_{before}_to_{after}")(col)
            df = df.withColumnRenamed(col, case_converted_col)
        return df

class MyDataset:
    """
    A class for creating synthetic dataset based on joining data types
    """
    def __init__(self, **kwargs) -> None:
        MyValidator.is_valid_dataframes(**kwargs)
        self.__dict__.update(kwargs)
        print(f"[MYDATASET_ATTRIBUTIONS]{self.__dict__}")

    def __repr__(self) -> str:
        return super().__repr__()



    def create_views(self):
        """
        A method for creating views of dataframes
        """
        for k, v in self.__dict__.items():
            if isinstance(v, dataframe.DataFrame):
                v.createOrReplaceTempView(f"{k}_view") # e.g. df_dmp_view

    def join_views(self):
        """
        A method for joining views based on each sql files of advertisers
        """
        query = f"""
            select *
            from df_propfit_view a
        """

    def create_synthetic_dataset(cls, dataset_type: str):
        pass

class MyVisualizer:
    """
    A class for creating visualizer to eda
    """

    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return super().__repr__()

class MyModel:
    """
    A class for creating model based on regression and classification
    """

    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return super().__repr__()

class MyPreprocessor:
    """
    A class for creating preprocessor to clean dataset
    """

    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return super().__repr__()

class MyTrainer:
    """
    A class for creating trainer
    """

    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return super().__repr__()


class MyEvaluator:
    """
    A class for creating evaluator
    """

    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return super().__repr__()


#####################################################
############ CREATE DATASET AND TRAINING ############
#####################################################

query = MyQuery.get_query("dmp")
df_dmp = spark.sql(query)
df_dmp = MyUtils.convert_case(df_dmp, "camel", "snake")

print(df_dmp.printSchema())

query = MyQuery.get_query("ga4")
df_ga4 = spark.sql(query)
df_ga4 = MyUtils.convert_case(df_ga4, "camel", "snake")

print(df_ga4.printSchema())

query = MyQuery.get_query("propfit")
df_propfit = spark.sql(query)
df_propfit = MyUtils.convert_case(df_propfit, "camel", "snake")

print(df_propfit.printSchema())

dataframes = {
    "df_dmp": df_dmp,
    "df_ga4": df_ga4,
    "df_propfit": df_propfit,
}

dataset = MyDataset(**dataframes)
dataset.create_views()
# spark.sql("select * from df_dmp_view limit 10").show()
query = MyQuery.get_query("join", "migun")
spark.sql(query).show(10)









