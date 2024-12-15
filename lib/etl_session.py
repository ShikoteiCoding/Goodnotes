from pyspark.sql import SparkSession
from pyspark import SparkConf

from lib.read import SparkFileReader


def init_spark_session(app_name: str, spark_config: dict = {}) -> SparkSession:
    spark_conf = SparkConf()
    spark_conf.setAppName(app_name)

    if spark_config:
        for k, v in spark_config.items():
            spark_conf.set(k, v)

    return SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()  # type: ignore


class ETLSession:

    def __init__(self, app_name: str, spark_config: dict[str, str] = {}):
        self.app_name = app_name
        self.spark_config = spark_config

        self._session: SparkSession = init_spark_session(
            self.app_name, self.spark_config
        )

    @property
    def session(self) -> SparkSession:
        return self._session

    @property
    def reader(self) -> SparkFileReader:
        return SparkFileReader(self.session)
