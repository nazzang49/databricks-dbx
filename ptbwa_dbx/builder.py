from dataclasses import dataclass
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

    def __init__(self, log_name: str = None, app_name: str = None, spark: SparkSession = None) -> None:
        self._log_name = log_name if log_name else DataType.DEFAULT_LOG_NAME.value
        self._app_name = app_name if app_name else DataType.DMP.value
        self._spark = spark if spark else SparkBuilder(app_name=self._app_name).spark

    @cached_property
    def log(self) -> Logger:
        """
        A method for setting custom log

        :return:
        """
        log4jLogger = self._spark.sparkContext._jvm.org.apache.log4j
        return log4jLogger.LogManager.getLogger(self._log_name)

@dataclass
class QueryArgs:
    """
    A class creating arguments to create query

    """

    table: str
    table_path: str
    database: str
    select: list
    where: dict = None
    orderby: dict = None
    groupby: list = None
    limit: int = None

    def __repr__(self):
        """
        A method for representing arguments by string-value

        :return:
        """
        return ('Field('
                f'table={self.table!r},'
                f'table_path={self.table_path!r},'
                f'database={self.database!r},'
                f'select={self.select!r},'
                f'where={self.where!r},'
                f'orderby={self.orderby!r},'
                f'groupby={self.groupby!r},'
                ')')

class QueryValidator:
    """
    A class for validating query before creation

    """

    _REQUIRED = {
        "is_valid_query_args": [
            "table",
            "table_path",
            "database",
            "select",
            "where",
            "orderby",
            "groupby",
            "limit",
        ]
    }

    _VALID_TABLES = [
        "streams_bid_bronze_app_nhn",
        "streams_imp_bronze_app_nhn",
        "streams_clk_bronze_app_nhn",
    ]

    def __init__(self, do_pre_validation: bool, do_post_validation: bool) -> None:
        self.do_pre_validation = do_pre_validation
        self.do_post_validation = do_post_validation

    def __call__(self, function):
        def inner(*args, **kwargs):
            method = function.__name__
            if self.do_pre_validation:
                QueryValidator._pre_validation(method)

            result = function(*args, **kwargs)

            if self.do_post_validation:
                QueryValidator._post_validation(result, method)

            return result
        return inner

    @staticmethod
    def _post_validation(result, method):
        """
        A method for validating after running method
        """

        if method not in QueryValidator._REQUIRED:
            raise ValueError(f"[INVALIDATE]METHOD::{method}")

        for k in QueryValidator._REQUIRED[method]:
            if k not in result.__dict__:
                raise ValueError(f"[NOT_FOUND]KEY::{k}")

            v = result.__dict__[k]
            if v and not isinstance(v, QueryArgs.__dataclass_fields__[k].type):
                raise ValueError(f"[TYPE_MISMATCH]KEY::{k}|TYPE::{QueryArgs.__dataclass_fields__[k].type}")
            
            if k != 'groupby' and v and not getattr(QueryValidator, f"is_valid_{k}")(v):
                raise ValueError(f"[INVALID_VALUE]KEY::{k}|VALUE::{v}")

        # (!) groupby cols should be in select cols
        conditions = result.__dict__['groupby']
        cols = result.__dict__["select"]
        if conditions and not QueryValidator.is_valid_groupby(cols, conditions):
            raise ValueError(f"[INVALID_VALUE]KEY::groupby|VALUE::{conditions}")

    @staticmethod
    def is_valid_table(table: str):
        """
        A class for validating table name

        :param table:
        :return:
        """
        return True if table in ["streams_bid_bronze_app_nhn"] else False

    @staticmethod
    def is_valid_database(database: str):
        """
        A class for validating database name

        :param database:
        :return:
        """
        return True if database in ["ice"] else False

    @staticmethod
    def is_valid_table_path(table_path: str):
        """
        A class for validating table path

        :param table_path:
        :return:
        """
        if "." not in table_path:
            return False

        database, table = table_path.split(".")
        if database not in ["ice"] or table not in ["streams_bid_bronze_app_nhn"]:
            return False
        return True

    @staticmethod
    def is_valid_select(cols: list):
        """
        A class for validating select cols

        :param cols:
        :return:
        """
        for col in cols:
            if not col.isalnum() and col != '*':
                return False
        return True

    @staticmethod
    def is_valid_where(conditions: dict):
        """
        A class for validating where conditions

        :param conditions:
        :return:
        """
        for k, v in conditions.items():
            for sub_k in k.split("_"):
                if not sub_k.isalnum():
                    return False
        return True

    @staticmethod
    def is_valid_orderby(conditions: dict):
        """
        A class for validating orderby conditions

        :param conditions:
        :return:
        """
        for k, v in conditions.items():
            if v not in ["asc", "desc"]:
                return False
        return True

    @staticmethod
    def is_valid_groupby(cols: list, conditions: list):
        """
        A class for validating groupby conditions

        :param cols:
        :param conditions:
        :return:
        """
        for condition in conditions:
            if condition and condition not in cols:
                return False
        return True

    @staticmethod
    def is_valid_limit(limit: int):
        """
        A class for validating limit condition (max 1000)

        :param limit:
        :return:
        """
        return True if limit <= 1000 else False

    @staticmethod
    def is_exist(command: str, **kwargs):
        if command not in kwargs:
            raise ValueError(f"[NOT_FOUND_{command.upper()}]REQUIRED")

class QueryBuilder:
    """
    A class for building query to validate fraud

    """

    _VALID_KEYS = [
        "table",
        "table_path",
        "database",
        "select",
        "where",
        "orderby",
        "groupby",
        "limit"
    ]

    _ADD_KEYS = [
        "where",
        "orderby",
        "groupby",
        "limit"
    ]

    def __init__(self, query_args: QueryArgs, spark: SparkSession) -> None:
        args = {
            k: v for k, v in query_args.__dict__.items() if k in self._VALID_KEYS
        }
        self.__dict__.update(args)
        self.is_valid_query_args()
        self.log = Logger(
            log_name=DataType.DEFAULT_LOG_NAME.value,
            app_name=DataType.DMP.value,
            spark=spark
        ).log

    @QueryValidator(do_pre_validation=False, do_post_validation=True)
    def is_valid_query_args(self):
        """
        A class for creating query arguments through validation

        :return:
        """

        # (!) consider load schema
        #



        self.project = DataType.DEFAULT_PROJECT.value
        self.assignee = DataType.DEFAULT_ASSIGNEE.value
        return self

    def _create_query(self):
        """
        A method for creating whole query with arguments
        """
        query_args = self.__dict__

        base_query = QueryBuilder.create_base_query(**query_args)
        for add_key in QueryBuilder._ADD_KEYS:
            base_query += getattr(QueryBuilder, f"add_{add_key}")(**query_args)

        self.log.info(f"[QUERY_BUILDER_RESULT]{base_query}")
        return base_query

    @staticmethod
    def create_base_query(**kwargs):
        if "select" not in kwargs or "table_path" not in kwargs:
            raise ValueError("[NOT_FOUND_REQUIRED]SELECT_OR_TABLE_PATH")
        return "SELECT " + ", ".join(kwargs["select"]) + " FROM " + kwargs["table_path"]

    @staticmethod
    def add_where(**kwargs):
        QueryValidator.is_exist("where", **kwargs)

        add_query = ""
        if kwargs["where"]:
            add_query += " WHERE "
            for i, (k, v) in enumerate(list(kwargs["where"].items())):
                add_query += (k + " " + v)
                add_query += " AND " if i != len(kwargs["where"]) - 1 else ""
        return add_query

    @staticmethod
    def add_orderby(**kwargs):
        QueryValidator.is_exist("orderby", **kwargs)

        add_query = ""
        if kwargs["orderby"]:
            add_query += " ORDER BY "
            for i, (k, v) in enumerate(list(kwargs["orderby"].items())):
                add_query += (k + " " + v)
                add_query += ", " if i != len(kwargs["orderby"]) - 1 else ""
        return add_query

    @staticmethod
    def add_groupby(**kwargs):
        QueryValidator.is_exist("groupby", **kwargs)

        add_query = ""
        if kwargs["groupby"]:
            add_query += " GROUP BY " + ", ".join(kwargs["groupby"])
        return add_query

    @staticmethod
    def add_limit(**kwargs):
        QueryValidator.is_exist("limit", **kwargs)

        add_query = ""
        if kwargs["limit"]:
            add_query += (" LIMIT " + str(kwargs["limit"]))
        return add_query

query_args = QueryArgs(
    table="streams_bid_bronze_app_nhn",
    database="ice",
    table_path="ice.streams_bid_bronze_app_nhn",
    select=["*"],
    where={
        "actiontime_local": ">= '2023-02-01'"
    },
)

spark = SparkBuilder().spark
query_builder = QueryBuilder(query_args, spark)
query = query_builder._create_query()

# (!) execute query test
df = spark.sql(query)
df.show(10)
