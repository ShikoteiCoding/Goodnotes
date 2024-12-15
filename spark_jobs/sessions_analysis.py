from pyspark.sql import DataFrame, functions as F, Window

from lib.etl_session import ETLSession

DATA_PATH = "data/"
USER_INTERACTIONS_SAMPLE = DATA_PATH + "user_interactions_sample.csv"


def compute_sessions(
    df: DataFrame,
    frame_size_minute: int = 30,
    date_field: str = "timestamp",
    key_field: str = "user_id",
) -> DataFrame:
    window = Window.partitionBy(key_field).orderBy(date_field)
    prev_date_field = f"prev_{date_field}"

    return (
        df.withColumn(prev_date_field, F.lag(date_field).over(window))
        .withColumn(
            "time_diff_min",
            (
                F.unix_timestamp(F.col(date_field))
                - F.unix_timestamp(F.col(prev_date_field))
            )
            / 60,
        )
        .withColumn(
            "is_new_session",
            F.when(
                (F.col("time_diff_min") > frame_size_minute)
                | F.col(prev_date_field).isNull(),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "session_id", F.sum("is_new_session").over(window)
        )  # Compute session id with cumulative sum over time of event being a new session
    )


if __name__ == "__main__":

    etl_session = ETLSession(
        "sessions_analysis",
        spark_config={
            "spark.sql.adaptive.enabled": "false",  # simulate no optimization
            "spark.ui.port": "4050",
        },
    )

    df = etl_session.reader.read_data(
        USER_INTERACTIONS_SAMPLE, "csv", spark_read_opts={"header": "true"}
    ).repartition(
        F.col("user_id")
    )  # explicit repartition

    sessions_df = compute_sessions(df, 30)

    # Session duration
    session_metrics_df = (
        sessions_df.groupBy("user_id", "session_id")
        .agg(
            (
                F.unix_timestamp(F.max("timestamp"))
                - F.unix_timestamp(F.min("timestamp"))
            ).alias("session_duration_secondes"),
            F.count("*").alias("actions_per_session"),
            F.first_value(F.col("duration_ms")).alias("duration_ms"),
        )
        .withColumn(
            "session_duration_secondes",
            F.when(
                F.col("session_duration_secondes") == 0, F.col("duration_ms") / 1_000
            ).otherwise("session_duration_secondes"),
        )
    )

    # Agregation of metrics
    user_metrics_df = session_metrics_df.groupBy("user_id").agg(
        F.count("session_id").alias("total_sessions"),
        F.avg(F.col("session_duration_secondes")).alias("avg_session_duration"),
    )

    user_metrics_df.write.mode("overwrite").parquet("output/session_analysis.parquet")

    user_metrics_df.explain(extended=True)

    # XXX: store results ?
