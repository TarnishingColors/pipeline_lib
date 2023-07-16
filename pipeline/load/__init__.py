import json
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Optional, Union
from enum import Enum
from ..utils import Level

import boto3
# import clickhouse_connect
import configparser
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F


path = Path(__file__).parents[1]
config_path = os.path.join(path, 'config.ini')
config = configparser.ConfigParser()
config.read(config_path)


class Table:
    def __init__(self, schema: str, table_name: str):
        self.schema = schema
        self.table_name = table_name

    def __str__(self):
        return f"{self.schema}.{self.table_name}"


# TODO: add partitioned tables handling
class BaseLoad(ABC):
    def __init__(
            self,
            level: Level,
            df: DataFrame,
            table: Table,
            spark: SparkSession
    ) -> None:
        self.level = level
        self.df = df.withColumn('utc_upload_dttm', F.lit(str(datetime.now())))
        self.table = table
        self.full_table_name = self.table.schema + '.' + self.table.table_name
        self.spark = spark

    def table_exists_assurance(self):
        schema = ', '.join(' '.join(x) for x in self.df.dtypes)
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.full_table_name}.({schema})")

    @abstractmethod
    def truncate_and_load(self, *args, **kwargs):
        self.table_exists_assurance()

    @abstractmethod
    def load_by_period(self, *args, **kwargs):
        self.table_exists_assurance()


class S3Load(BaseLoad):
    def __init__(
            self,
            level: Level,
            df: Union[dict, DataFrame],
            table: Table,
            spark: Optional[SparkSession] = None
    ) -> None:
        if spark:
            super().__init__(df, table, spark)
        else:
            self.level = level
            self.df = df
            self.table = table
            self.full_table_name = self.table.schema + '.' + self.table.table_name

    def upload_data_as_json(self, days_delta: int = 0):
        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            endpoint_url=config.get('s3', 'fs.s3a.endpoint'),
            aws_access_key_id=config.get('s3', 'fs.s3a.access.key'),
            aws_secret_access_key=config.get('s3', 'fs.s3a.secret.key')
        )

        date_upload = datetime.now() + timedelta(days_delta)

        s3.put_object(
            Body=json.dumps(self.df),
            Bucket=config.get('s3', 'bucket_name'),
            Key=f'{self.level.name}/{self.table.schema}/{self.table.table_name}_{date_upload.strftime("%Y-%m-%d")}.json'
        )

    def truncate_and_load(self, *args, **kwargs):
        super().replace_by_snapshot()

    def load_by_period(self, *args, **kwargs):
        pass


class HiveLoad(BaseLoad):
    def __init__(self, level: Level, df: DataFrame, table: Table, spark: SparkSession) -> None:
        super().__init__(df, table, spark)

    def truncate_and_load(self, *args, **kwargs):
        super().replace_by_snapshot()

        self.spark.sql(f"TRUNCATE TABLE {self.full_table_name}")

        self.df.createOrReplaceTempView(self.table.table_name)
        self.spark.sql(f"INSERT INTO TABLE {self.full_table_name} SELECT * FROM {self.table.table_name}")

    def load_by_period(self, *args, **kwargs):
        pass


class ClickhouseLoad(BaseLoad):
    def __init__(self, level: Level, df: DataFrame, table: Table, spark: SparkSession) -> None:
        super().__init__(df, table, spark)

    def truncate_and_load(self, *args, **kwargs):
        super().replace_by_snapshot()

    def load_by_period(self, *args, **kwargs):
        pass
