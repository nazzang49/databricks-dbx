from enum import Enum

class CommonType(Enum):
    """
    A class for using common types e.g. ptbwa

    """
    DEFAULT_APP_NAME = "ptbwa"
    DEFAULT_LOG_NAME = "ptbwa"
    DEFAULT_PROJECT = "propfit"
    DEFAULT_ASSIGNEE = "jinyoung.park@ptbwa.com"

class DataType(Enum):
    """
    A class for using propfit types e.g. ml

    """
    PROPFIT = "propfit"
    DMP = "dmp"
    GA4 = "ga4"
