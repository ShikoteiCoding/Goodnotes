from pyspark.sql import DataFrame, functions as F
from random import randint

from lib.etl_session import ETLSession

DATA_PATH = "data/"
USER_INTERACTIONS_SAMPLE = DATA_PATH + "user_interactions_sample.csv"
USER_METADATA_SAMPLE = DATA_PATH + "user_metadata_sample.csv"

if __name__ == "__main__":
    etl_session = ETLSession(
        "active_metadata_join",
    )

    user_df = etl_session.reader.read_data(
        USER_INTERACTIONS_SAMPLE, "csv", spark_read_opts={"header": "true"}
    )

    metadata_df = etl_session.reader.read_data(
        USER_METADATA_SAMPLE, "csv", spark_read_opts={"header": "true"}
    )

    user_enriched_df = user_df.join(metadata_df, on=["user_id"])
    # user_enriched_df = user_df.join(metadata_df.hint("shuffle_hash"), on=["user_id"]) # another un optimized solution

    # XXX: store results ?
    user_enriched_df.explain()
