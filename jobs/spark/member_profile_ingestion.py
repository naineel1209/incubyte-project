from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from member_profile_exports import write_country_targets


INCOMING_MEMBER_DIR = Path("/opt/skypoints-data/incoming/member")
CONTROL_TABLE_PATH = "/opt/skypoints-data/control/member_profile_control"
RAW_MEMBER_PATH = "/opt/skypoints-data/delta/raw/member_profile"
STAGING_MEMBER_PATH = "/opt/skypoints-data/delta/staging/member_profile"
TARGET_MEMBER_PATH = "/opt/skypoints-data/delta/target/member_profile"


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

    # As per Design document, the raw record is a pipe-separated string, so we split it into fields and create a new DataFrame with the required columns
    # 0 - empty, 1 - record_type, 2 - name, 3 - mem_id, 4 - enroll_dt, 5 - flight_dt, 6 - tier, 7 - agent_name, 8 - state, 9 - country_code, 10 - dob, 11 - flag
    # Appropriately fill the columns with the required data types
    fields = F.split(F.col("raw_record"), r"\|")
    decoded_member_dataframe = (
        raw_member_dataframe.withColumn("fields", fields)
        .filter(F.col("fields").getItem(1) == "D")
        .select(
            "source_path",
            "ingestion_time",
            "source_file_date",
            F.col("fields").getItem(2).alias("name"),
            F.col("fields").getItem(3).alias("mem_id"),
            F.to_date(F.col("fields").getItem(4), "yyyyMMdd").alias("enroll_dt"), # yyyy (2002) - MM (09, 11) - dd (01, 02, 03)
            F.to_date(F.col("fields").getItem(5), "yyyyMMdd").alias("flight_dt"),
            F.col("fields").getItem(6).alias("tier"),
            F.col("fields").getItem(7).alias("agent_name"),
            F.col("fields").getItem(8).alias("state"),
            F.col("fields").getItem(9).alias("country_code"),
            F.coalesce(
                F.to_date(F.col("fields").getItem(10), "yyyyMMdd"),
                F.to_date(F.col("fields").getItem(10), "MMddyyyy"),
            ).alias("dob"),
            F.col("fields").getItem(11).alias("flag"),
        )
        .filter(F.col("mem_id").isNotNull())
    )
    # Technical Assessment: Deliverables: 2 - Age (computed from DOB) and a Stale_Member flag where days since Flight_Date > 90.
    staging_member_dataframe = decoded_member_dataframe.withColumns(
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
    )

    # Write the raw and staging records before updating the target.
    raw_member_dataframe.write.format("delta").mode("append").save(RAW_MEMBER_PATH)
    staging_member_dataframe.write.format("delta").mode("append").save(
        STAGING_MEMBER_PATH
    )

    # Keep one latest record per member within the current run.
    ranking_window = Window.partitionBy("mem_id").orderBy(
        F.col("source_file_date").desc(),
        F.col("ingestion_time").desc(),
        F.col("source_path").desc(),
    )
    current_batch_latest = (
        staging_member_dataframe
        .withColumn("row_number", F.row_number().over(ranking_window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
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
