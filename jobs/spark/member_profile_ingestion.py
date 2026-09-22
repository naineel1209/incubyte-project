from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_validations import (
    MEMBER_QUARANTINE_PATH,
    add_member_validation_errors,
    add_member_structure_validation_errors,
    assert_unique_keys,
    latest_member_records,
    write_quarantine,
)
from member_profile_exports import write_country_targets


INCOMING_MEMBER_DIR = Path("/opt/skypoints-data/incoming/member")
CONTROL_TABLE_PATH = "/opt/skypoints-data/control/member_profile_control"
RAW_MEMBER_PATH = "/opt/skypoints-data/delta/raw/member_profile"
STAGING_MEMBER_PATH = "/opt/skypoints-data/delta/staging/member_profile"
TARGET_MEMBER_PATH = "/opt/skypoints-data/delta/target/member_profile"
EXPECTED_MEMBER_HEADER = (
    "|H|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|Tier_Code|"
    "Agent_Name|State|Country|DOB|Is_Active"
)


def main() -> None:
    spark = SparkSession.builder.appName("MemberProfileIngestion").getOrCreate()

    # Check control table existence and create if it doesn't exist
    if not DeltaTable.isDeltaTable(spark, CONTROL_TABLE_PATH):
        (
            spark.createDataFrame([], "source_path STRING")
            .write.format("delta")
            .mode("errorifexists")
            .save(CONTROL_TABLE_PATH)
        )

    # Find all the files in the incoming member directory that have not been ingested yet
    member_control = DeltaTable.forPath(spark, CONTROL_TABLE_PATH)
    ingested_paths = {
        row.source_path
        for row in member_control.toDF().select("source_path").collect()
    }
    files_to_ingest = [
        file_path
        for file_path in sorted(INCOMING_MEMBER_DIR.glob("*.txt"))
        if str(file_path) not in ingested_paths
    ]

    if not files_to_ingest:
        spark.stop()
        return

    # Read all raw records as text and create a DataFrame with source_path, raw_record, and ingestion_time
    file_dataframes = [
        spark.read.text(str(file_path)).select(
            F.lit(str(file_path)).alias("source_path"),
            F.col("value").alias("raw_record"),
            F.to_date(
                F.lit(file_path.stem.rsplit("_", 1)[-1]), "yyyyMMdd"
            ).alias("source_file_date"),
            F.current_timestamp().alias("ingestion_time")
        )
        for file_path in files_to_ingest
    ]

    # Union all the DataFrames into a single DataFrame
    raw_member_dataframe = file_dataframes[0]

    for df in file_dataframes[1:]:
        raw_member_dataframe = raw_member_dataframe.unionByName(df)

    # Write every source row to the raw layer before validation.
    raw_member_dataframe.write.format("delta").mode("append").save(RAW_MEMBER_PATH)

    # Technical Assessment: Deliverable 5 - validate structure and quarantine malformed records before staging.
    fields = F.split(F.col("raw_record"), r"\|", -1)
    parsed_member_rows = (
        raw_member_dataframe.withColumn("fields", fields)
        .withColumn("record_type", F.col("fields").getItem(1))
        .withColumn("field_count", F.size("fields"))
    )
    structural_quarantine = add_member_structure_validation_errors(
        parsed_member_rows, EXPECTED_MEMBER_HEADER
    )
    structural_quarantine = (
        structural_quarantine.filter(F.size("validation_errors") > 0)
        .drop("fields")
    )
    write_quarantine(structural_quarantine, MEMBER_QUARANTINE_PATH)

    detail_rows = parsed_member_rows.filter(
        (F.col("record_type") == "D") & (F.col("field_count") == 12)
    )

    def clean_field(field_position):
        field = F.trim(F.col("fields").getItem(field_position))
        return F.when(F.length(field) > 0, field).otherwise(F.lit(None))

    decoded_member_dataframe = (
        detail_rows.select(
            "source_path",
            "raw_record",
            "ingestion_time",
            "source_file_date",
            "field_count",
            clean_field(2).alias("name"),
            clean_field(3).alias("mem_id"),
            clean_field(4).alias("enroll_dt_raw"),
            clean_field(5).alias("flight_dt_raw"),
            clean_field(6).alias("tier"),
            clean_field(7).alias("agent_name"),
            clean_field(8).alias("state"),
            clean_field(9).alias("country_code"),
            clean_field(10).alias("dob_raw"),
            clean_field(11).alias("flag"),
        )
        .withColumn(
            "enroll_dt",
            F.to_date(F.col("enroll_dt_raw"), "yyyyMMdd"),
        )
        .withColumn(
            "flight_dt",
            F.to_date(F.col("flight_dt_raw"), "yyyyMMdd"),
        )
        .withColumn(
            "dob",
            F.coalesce(
                F.to_date(F.col("dob_raw"), "yyyyMMdd"),
                F.to_date(F.col("dob_raw"), "MMddyyyy"),
            ),
        )
        .withColumn("mem_id", F.trim(F.col("mem_id")))
        .withColumn("country_code", F.upper(F.trim(F.col("country_code"))))
        .withColumn("flag", F.upper(F.trim(F.col("flag"))))
    )
    validated_member_dataframe = add_member_validation_errors(
        decoded_member_dataframe
    )
    write_quarantine(
        validated_member_dataframe.filter(F.size("validation_errors") > 0),
        MEMBER_QUARANTINE_PATH,
    )
    valid_member_dataframe = validated_member_dataframe.filter(
        F.size("validation_errors") == 0
    ).drop("validation_errors")

    # Technical Assessment: Deliverables: 2 - Age (computed from DOB) and a Stale_Member flag where days since Flight_Date > 90.
    staging_member_dataframe = valid_member_dataframe.withColumns(
        {
            "age": F.floor(F.months_between(F.current_date(), F.col("dob")) / 12).cast(
                "int"
            ),
            "stale_member": F.when(
                F.col("flight_dt").isNotNull()
                & (F.datediff(F.current_date(), F.col("flight_dt")) > 90),
                F.lit(True),
            ).otherwise(F.lit(False)),
        }
    ).select(
        "source_path",
        "ingestion_time",
        "source_file_date",
        "name",
        "mem_id",
        "enroll_dt",
        "flight_dt",
        "tier",
        "agent_name",
        "state",
        "country_code",
        "dob",
        "flag",
        "age",
        "stale_member",
    )

    # Write valid records to staging before updating the target.
    has_valid_member_records = staging_member_dataframe.limit(1).count() > 0
    if has_valid_member_records:
        staging_member_dataframe.write.format("delta").mode("append").save(
            STAGING_MEMBER_PATH
        )

    if has_valid_member_records:
        # Keep one latest record per member within the current run.
        current_batch_latest = latest_member_records(staging_member_dataframe)
        assert_unique_keys(
            current_batch_latest, ["mem_id"], "member_profile_current_batch"
        )

        if not DeltaTable.isDeltaTable(spark, TARGET_MEMBER_PATH):
            (
                current_batch_latest.write.format("delta")
                .mode("overwrite")
                .partitionBy("country_code")
                .save(TARGET_MEMBER_PATH)
            )
        else:
            target_table = DeltaTable.forPath(spark, TARGET_MEMBER_PATH)
            assert_unique_keys(
                target_table.toDF(), ["mem_id"], "member_profile_target_before_merge"
            )

            # Simple condition - rank records by source_file_date and ingestion_time - if the source record is newer than the target record, update the target record with the source record.
            newer_record = """
                source.source_file_date > target.source_file_date
                OR (
                    source.source_file_date = target.source_file_date
                    AND source.ingestion_time > target.ingestion_time
                )
            """

            (
                target_table.alias("target")
                .merge(
                    current_batch_latest.alias("source"),
                    "target.mem_id = source.mem_id",
                )
                .whenMatchedUpdateAll(condition=newer_record)
                .whenNotMatchedInsertAll()
                .execute()
            )

        assert_unique_keys(
            spark.read.format("delta").load(TARGET_MEMBER_PATH),
            ["mem_id"],
            "member_profile_target",
        )

        # Technical Assessment: Deliverable 3: Country Specific tables write - Chose Overwrite here - as some members might have moved countries - so our target table got updated - to have that data flow through, we can simply overwrite the country specific tables -
        # A better approach would've been using views but since the requirement asks us specifically "country specific tables", I have chose this option
        write_country_targets(spark)

    # Mark files as processed only after the target update succeeds.
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
