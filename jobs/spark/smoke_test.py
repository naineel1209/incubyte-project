from __future__ import annotations

from pyspark.sql import SparkSession


OUTPUT_PATH = "file:///opt/skypoints-data/delta/smoke/member_profile"


def main() -> None:
    spark = SparkSession.builder.appName("skypoints-delta-smoke-test").getOrCreate()

    rows = [
        ("223457", "Elena", "USA"),
        ("223458", "Ravi", "IND"),
        ("223459", "Mateo", "PHL"),
    ]
    member_df = spark.createDataFrame(
        rows,
        ["member_id", "member_name", "country_code"],
    )

    (
        member_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("country_code")
        .save(OUTPUT_PATH)
    )

    saved_df = spark.read.format("delta").load(OUTPUT_PATH)
    print(f"Delta rows written: {saved_df.count()}")
    saved_df.orderBy("member_id").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
