from pyspark.sql import SparkSession, DataFrame, DataFrameReader
from pyspark.sql.types import StructType


class SparkFileReader:

    def __init__(self, spark_session: SparkSession):
        self._spark_session = spark_session

    def read_data(
        self,
        path: str,
        file_format: str,
        spark_read_opts: dict[str, str] | None = None,
        schema: StructType | None = None,
    ) -> DataFrame:
        print(f"reading data from {path}")

        if file_format not in ["csv"]:
            raise ValueError(f"format {file_format} not allowed")

        reader: DataFrameReader
        if schema:
            reader = self._spark_session.read.schema(schema)
        else:
            reader = self._spark_session.read

        if spark_read_opts:
            reader = reader.options(**spark_read_opts)

        return reader.load(path, format=file_format)
