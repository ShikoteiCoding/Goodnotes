from pyspark.sql import DataFrame, functions as F
from datetime import datetime, timezone

from lib.etl_session import ETLSession

DATA_PATH = "data/"
USER_INTERACTIONS_SAMPLE = DATA_PATH + "user_interactions_sample.csv"


def compute_activity(
    df: DataFrame, unit: str, date_field: str = "timestamp", key_field: str = "user_id"
) -> DataFrame:
    return df.groupBy(F.date_trunc(unit, date_field).alias(unit)).agg(
        F.countDistinct(key_field).alias("count_active_users")
    )


if __name__ == "__main__":
    etl_session = ETLSession(
        "daily_active_users", spark_config={"spark.sql.adaptive.enabled": "true"}
    )

    df = etl_session.reader.read_data(
        USER_INTERACTIONS_SAMPLE, "csv", spark_read_opts={"header": "true"}
    ).repartition(F.col("user_id"))

    # past year
    past_year = datetime.now(tz=timezone.utc).year - 1
    df = df.filter((F.year("timestamp") == F.lit(past_year)))

    df = df.filter(
        (F.col("duration_ms").isNull())
        | ((F.col("duration_ms") > 10_000) & (F.col("duration_ms") < 3_600 * 1_000))
    )

    # metrics compute
    dau_df = compute_activity(df, "day")
    mau_df = compute_activity(df, "month")

    # XXX: store results ?

    dau_df.show(truncate=False, vertical=True)
    mau_df.show(truncate=False)
