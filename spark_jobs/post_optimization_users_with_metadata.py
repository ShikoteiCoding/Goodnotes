from pyspark.sql import DataFrame, functions as F
from random import randint

from lib.etl_session import ETLSession

DATA_PATH = "data/"
USER_INTERACTIONS_SAMPLE = DATA_PATH + "user_interactions_sample.csv"
USER_METADATA_SAMPLE = DATA_PATH + "user_metadata_sample.csv"

if __name__ == "__main__":
    etl_session = ETLSession(
        "active_metadata_join",
        spark_config={
            "spark.sql.join.preferSortMergeJoin": "true",
            "spark.sql.autoBroadcastJoinThreshold": "0",  # force disable broadcast
        },
    )

    user_df = etl_session.reader.read_data(
        USER_INTERACTIONS_SAMPLE, "csv", spark_read_opts={"header": "true"}
    )

    salted_user_df = user_df.withColumn("salt", F.floor(F.rand() * 8))

    metadata_df = etl_session.reader.read_data(
        USER_METADATA_SAMPLE, "csv", spark_read_opts={"header": "true"}
    )

    salted_metadata_df = metadata_df.withColumn(
        "salt", F.expr("explode(array(0, 1, 2, 3, 4, 5, 6, 7))")
    )

    user_enriched_df = salted_user_df.join(
        salted_metadata_df, on=["user_id", "salt"]
    ).drop("salt")

    # XXX: store results ?
    user_enriched_df.explain()
