from __future__ import annotations

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


INCOMING_REDEMPTION_DIR = Path("/opt/skypoints-data/incoming/redemption")
CONTROL_TABLE_PATH = "/opt/skypoints-data/control/redemption_file_control"
REDEMPTION_TARGET_PATH = "/opt/skypoints-data/delta/target/redemption_transactions"

REDEMPTION_SCHEMA = T.StructType(
    [
        T.StructField("member_id", T.StringType(), True),
        T.StructField("feed_date", T.StringType(), True),
        T.StructField(
            "redemptions",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("txn_id", T.StringType(), True),
                        T.StructField("txn_date", T.StringType(), True),
                        T.StructField("partner", T.StringType(), True),
                        T.StructField("miles_redeemed", T.LongType(), True),
                        T.StructField("status", T.StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
    ]
)


def read_flattened_redemptions(spark: SparkSession, files_to_ingest: list[Path]):
    file_dataframes = []
    for file_path in files_to_ingest:
        file_dataframe = (
            spark.read.option("multiline", "true")
            .schema(REDEMPTION_SCHEMA)
            .json(str(file_path))
            # exploding redemptions as they are an array
            .select(
                "member_id",
                F.to_date(F.col("feed_date"), "yyyyMMdd").alias("feed_date"),
                F.explode("redemptions").alias("redemption"),
                F.lit(str(file_path)).alias("source_path"),
                F.current_timestamp().alias("ingestion_time"),
            )
            # simple json fields selectors (.) operators
            .select(
                "member_id",
                "feed_date",
                F.col("redemption.txn_id").alias("txn_id"),
                F.to_date(
                    F.col("redemption.txn_date"), "yyyyMMdd"
                ).alias("txn_date"),
                F.col("redemption.partner").alias("partner"),
                F.col("redemption.miles_redeemed").alias("miles_redeemed"),
                F.col("redemption.status").alias("status"),
                "source_path",
                "ingestion_time",
            )
            .filter(F.col("member_id").isNotNull())
            .filter(F.col("txn_id").isNotNull())
        )
        file_dataframes.append(file_dataframe)

    flattened_dataframe = file_dataframes[0]
    for file_dataframe in file_dataframes[1:]:
        flattened_dataframe = flattened_dataframe.unionByName(file_dataframe)
    return flattened_dataframe

# Technical Assessment: Deliverable 4: Redemption Feed
def main() -> None:
    spark = SparkSession.builder.appName("RedemptionIngestion").getOrCreate()

    if not DeltaTable.isDeltaTable(spark, CONTROL_TABLE_PATH):
        (
            spark.createDataFrame([], "source_path STRING")
            .write.format("delta")
            .mode("overwrite")
            .save(CONTROL_TABLE_PATH)
        )

    control_table = DeltaTable.forPath(spark, CONTROL_TABLE_PATH)
    ingested_paths = {
        row.source_path
        for row in control_table.toDF().select("source_path").collect()
    }
    files_to_ingest = [
        file_path
        for file_path in sorted(INCOMING_REDEMPTION_DIR.glob("*.json"))
        if str(file_path) not in ingested_paths
    ]

    if not files_to_ingest:
        spark.stop()
        return

    flattened_dataframe = read_flattened_redemptions(spark, files_to_ingest)
    # Ranking on member_id and txn_id - visibly the only identifier but statuses can change and basing on an assumption - feed_date
    status_priority = (
        F.when(F.upper(F.col("status")) == "COMPLETED", F.lit(2))
        .when(F.upper(F.col("status")) == "PENDING", F.lit(1))
        .otherwise(F.lit(0))
    )
    ranking_window = Window.partitionBy("member_id", "txn_id").orderBy(
        status_priority.desc(),
        F.col("feed_date").desc_nulls_last(),
        F.col("ingestion_time").desc_nulls_last(),
        F.col("source_path").desc(),
    )
    current_batch_latest = (
        flattened_dataframe.withColumn(
            "row_number", F.row_number().over(ranking_window)
        )
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )

    if not DeltaTable.isDeltaTable(spark, REDEMPTION_TARGET_PATH):
        (
            current_batch_latest.write.format("delta")
            .mode("overwrite")
            .partitionBy("feed_date")
            .save(REDEMPTION_TARGET_PATH)
        )
    else:
        target_table = DeltaTable.forPath(spark, REDEMPTION_TARGET_PATH)
        # New record has higher priority - wins
        # New record has same priority - wins
        newer_record = """
            CASE UPPER(source.status)
                WHEN 'COMPLETED' THEN 2
                WHEN 'PENDING' THEN 1
                ELSE 0
            END > CASE UPPER(target.status)
                WHEN 'COMPLETED' THEN 2
                WHEN 'PENDING' THEN 1
                ELSE 0
            END
            OR (
                CASE UPPER(source.status)
                    WHEN 'COMPLETED' THEN 2
                    WHEN 'PENDING' THEN 1
                    ELSE 0
                END = CASE UPPER(target.status)
                    WHEN 'COMPLETED' THEN 2
                    WHEN 'PENDING' THEN 1
                    ELSE 0
                END
                AND (
                    source.feed_date > target.feed_date
                    OR (
                        source.feed_date = target.feed_date
                        AND source.ingestion_time > target.ingestion_time
                    )
                )
            )
        """
        (
            target_table.alias("target")
            .merge(
                current_batch_latest.alias("source"),
                "target.member_id = source.member_id AND target.txn_id = source.txn_id",
            )
            .whenMatchedUpdateAll(condition=newer_record)
            .whenNotMatchedInsertAll()
            .execute()
        )

    (
        spark.createDataFrame(
            [(str(path),) for path in files_to_ingest], "source_path STRING"
        )
        .write.format("delta")
        .mode("append")
        .save(CONTROL_TABLE_PATH)
    )
    spark.stop()


if __name__ == "__main__":
    main()
