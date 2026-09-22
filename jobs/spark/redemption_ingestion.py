from __future__ import annotations

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from data_validations import (
    REDEMPTION_QUARANTINE_PATH,
    add_redemption_validation_errors,
    assert_unique_keys,
    combine_validation_errors,
    latest_redemption_records,
    validation_error,
    write_quarantine,
)


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
        ),
        T.StructField("_corrupt_record", T.StringType(), True),
    ]
)


def read_flattened_redemptions(spark: SparkSession, files_to_ingest: list[Path]):
    file_dataframes = []
    invalid_feed_dataframes = []

    for file_path in files_to_ingest:
        file_dataframe = (
            spark.read.option("multiline", "true")
            .schema(REDEMPTION_SCHEMA)
            .json(str(file_path))
            .withColumn("source_path", F.lit(str(file_path)))
            .withColumn("ingestion_time", F.current_timestamp())
            .withColumn("feed_date_raw", F.trim(F.col("feed_date")))
            .withColumn("feed_date", F.to_date(F.col("feed_date"), "yyyyMMdd"))
        )

        feed_errors = combine_validation_errors(
            validation_error(
                F.col("_corrupt_record").isNotNull(), "INVALID_REDEMPTION_JSON"
            ),
            validation_error(
                F.col("member_id").isNull()
                | (F.length(F.trim(F.col("member_id"))) == 0),
                "MISSING_MEMBER_ID",
            ),
            validation_error(
                (F.length(F.trim(F.col("member_id"))) > 18)
                | (~F.trim(F.col("member_id")).rlike(r"^[0-9]+$")),
                "INVALID_MEMBER_ID",
            ),
            validation_error(
                F.col("feed_date_raw").isNull()
                | (F.length(F.col("feed_date_raw")) == 0),
                "MISSING_FEED_DATE",
            ),
            validation_error(
                F.col("feed_date_raw").isNotNull()
                & (F.length(F.col("feed_date_raw")) > 0)
                & F.col("feed_date").isNull(),
                "INVALID_FEED_DATE",
            ),
            validation_error(
                F.col("feed_date").isNotNull()
                & (F.col("feed_date") > F.current_date()),
                "FUTURE_FEED_DATE",
            ),
            validation_error(
                F.col("redemptions").isNull(), "MISSING_REDEMPTIONS_ARRAY"
            ),
        )
        invalid_feed_dataframes.append(
            file_dataframe.withColumn("validation_errors", feed_errors).filter(
                F.size("validation_errors") > 0
            )
        )

        # Explode the redemption array into one queryable row per transaction.
        file_dataframes.append(
            file_dataframe.filter(F.size("redemptions") > 0)
            .select(
                "member_id",
                "feed_date_raw",
                "feed_date",
                F.explode("redemptions").alias("redemption"),
                "source_path",
                "ingestion_time",
            )
            .select(
                F.trim(F.col("member_id")).alias("member_id"),
                "feed_date_raw",
                "feed_date",
                F.trim(F.col("redemption.txn_id")).alias("txn_id"),
                F.trim(F.col("redemption.txn_date")).alias("txn_date_raw"),
                F.to_date(
                    F.col("redemption.txn_date"), "yyyyMMdd"
                ).alias("txn_date"),
                F.trim(F.col("redemption.partner")).alias("partner"),
                F.col("redemption.miles_redeemed").alias("miles_redeemed"),
                F.upper(F.trim(F.col("redemption.status"))).alias("status"),
                "source_path",
                "ingestion_time",
            )
        )

    flattened_dataframe = file_dataframes[0]
    for file_dataframe in file_dataframes[1:]:
        flattened_dataframe = flattened_dataframe.unionByName(file_dataframe)

    invalid_feed_dataframe = invalid_feed_dataframes[0]
    for invalid_dataframe in invalid_feed_dataframes[1:]:
        invalid_feed_dataframe = invalid_feed_dataframe.unionByName(invalid_dataframe)

    return flattened_dataframe, invalid_feed_dataframe


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

    flattened_dataframe, invalid_feed_dataframe = read_flattened_redemptions(
        spark, files_to_ingest
    )
    write_quarantine(invalid_feed_dataframe, REDEMPTION_QUARANTINE_PATH)

    validated_redemption_dataframe = add_redemption_validation_errors(
        flattened_dataframe
    )
    write_quarantine(
        validated_redemption_dataframe.filter(F.size("validation_errors") > 0),
        REDEMPTION_QUARANTINE_PATH,
    )
    valid_redemption_dataframe = validated_redemption_dataframe.filter(
        F.size("validation_errors") == 0
    ).drop("validation_errors")

    # Ranking on member_id and txn_id - visibly the only identifier but statuses can change and basing on an assumption - feed_date
    current_batch_latest = (
        latest_redemption_records(valid_redemption_dataframe)
        .select(
            "member_id",
            "txn_id",
            "feed_date",
            "txn_date",
            "partner",
            "miles_redeemed",
            "status",
            "source_path",
            "ingestion_time",
        )
    )

    has_valid_redemptions = current_batch_latest.limit(1).count() > 0
    if has_valid_redemptions:
        assert_unique_keys(
            current_batch_latest,
            ["member_id", "txn_id"],
            "redemption_transactions_current_batch",
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
            assert_unique_keys(
                target_table.toDF(),
                ["member_id", "txn_id"],
                "redemption_transactions_target_before_merge",
            )
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

        assert_unique_keys(
            spark.read.format("delta").load(REDEMPTION_TARGET_PATH),
            ["member_id", "txn_id"],
            "redemption_transactions_target",
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
