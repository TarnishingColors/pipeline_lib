from abc import ABC, abstractmethod
from datetime import datetime
from functools import wraps
import configparser
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


path = Path(__file__).parents[1]
config_path = os.path.join(path, 'config.ini')
config = configparser.ConfigParser()
config.read(config_path)


def date_filter(func):
    """Wrapper to filter DataFrame by given dates"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        if isinstance(start_date, datetime):
            df = df.where(df.utc_created_dttm >= start_date)
        if isinstance(end_date, datetime):
            df = df.where(df.utc_created_dttm <= end_date)
        return df
    return wrapper


class BaseExtract(ABC):
    """Base class for data extraction"""
    def __init__(self,
                 app_name: str,
                 start_date: datetime = None,
                 end_date: datetime = None,
                 enable_hive_support: bool = False
                 ) -> None:
        if enable_hive_support:
            self.spark = (SparkSession
                          .builder
                          .appName(app_name)
                          .master("local[*]")
                          .config("spark.hadoop.hive.exec.dynamic.partition", "true")
                          .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
                          .enableHiveSupport()
                          .getOrCreate()
                          )
        else:
            self.spark = SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()
        self.start_date = start_date
        self.end_date = end_date

    @abstractmethod
    def extract(self, start_date: datetime = None, end_date: datetime = None, *args, **kwargs) -> DataFrame:
        pass


class S3Extract(BaseExtract):
    """Class for extracting files from S3 (regardless of cloud vendor)"""
    def __init__(self, app_name: str, start_date: datetime = None, end_date: datetime = None) -> None:
        super().__init__(app_name, start_date=start_date, end_date=end_date)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", config.get('s3', 'fs.s3a.endpoint'))
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.signing-algorithm", config.get('s3', 'fs.s3a.signing-algorithm'))
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", config.get('s3', 'fs.s3a.aws.credentials.provider'))
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", config.get('s3', 'fs.s3a.access.key'))
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config.get('s3', 'fs.s3a.secret.key'))

    # TODO: add handling other input formats (besides json)
    @date_filter
    def extract(self, start_date: datetime = None, end_date: datetime = None, *args, **kwargs) -> DataFrame:
        super().extract(start_date=self.start_date, end_date=self.end_date, *args, **kwargs)
        df = self.spark.read.json(
            f"s3a://{config.get('s3', 'bucket_name')}/{kwargs['file_dir']}"
        )
        return df


class MongodbExtract(BaseExtract):
    """Class for extracting records from Mongodb"""
    def __init__(self, app_name: str, start_date: datetime = None, end_date: datetime = None) -> None:
        super().__init__(app_name, start_date=start_date, end_date=end_date)

    @date_filter
    def extract(self, *args, **kwargs) -> DataFrame:
        super().extract(*args, **kwargs)
        df = self.spark.read.format("mongo").option("uri", kwargs['collection_uri']).load()
        return df


class HiveExtract(BaseExtract):
    """Class for extracting rows from Hive"""
    def __init__(self, app_name: str, start_date: datetime = None, end_date: datetime = None) -> None:
        super().__init__(app_name, start_date=start_date, end_date=end_date, enable_hive_support=True)

    @date_filter
    def extract(self, *args, **kwargs) -> DataFrame:
        super().extract(*args, **kwargs)
        df = self.spark.sql(f"SELECT * FROM {kwargs['schema']}.{kwargs['table']}")
        return df
