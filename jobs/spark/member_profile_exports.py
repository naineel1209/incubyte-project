from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


TARGET_MEMBER_PATH = "/opt/skypoints-data/delta/target/member_profile"
COUNTRY_EXPORT_PATH = Path("/opt/skypoints-data/exports/member")

COUNTRY_COLUMNS = [
    "mem_id",
    "name",
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
]

def usa_export_function(spark: SparkSession, usa_df):
    """
    Export the USA member profile data to a CSV file.

    Dimensions of file:
        - ID - mem_id
        - Name - name
        - TierCode - tier
        - EnrollmentDate - enroll_dt - (format: MddYYYY)
    """

    us_pd_df = usa_df.select(
        F.col("mem_id").alias("ID"),
        F.col("name").alias("Name"),
        F.col("tier").alias("TierCode"),
        F.date_format(F.col("enroll_dt"), "Mddyyyy").alias("EnrollmentDate"),
    ).toPandas()

    us_pd_df.to_csv(
        COUNTRY_EXPORT_PATH / "usa" / "member_profile_usa.csv", index=False
    )


def india_export_function(spark: SparkSession, india_df):
    """
    Export the India member profile data to a CSV file.

    Dimensions of file:
        - ID - mem_id
        - Name - name
        - DOB - dob - (format: M/dd/yyyy)
        - TierCode - tier
        - EnrollmentDate - enroll_dt - (format: MddYYYY)
        - Individual or Corporate - (unknown column, don't know source - NULL for now)
        - Flight Date - flight_dt - (format: MddYYYY)
    """

    ind_pd_df = india_df.select(
        F.col("mem_id").alias("ID"),
        F.col("name").alias("Name"),
        F.date_format(F.col("dob"), "M/dd/yyyy").alias("DOB"),
        F.col("tier").alias("TierCode"),
        F.date_format(F.col("enroll_dt"), "Mddyyyy").alias("EnrollmentDate"),
        F.lit(None).alias("Individual_or_Corporate"),
        F.date_format(F.col("flight_dt"), "Mddyyyy").alias("Flight_Date"),
    ).toPandas()

    # Fix the column names with spaces in Pandas Dataframe
    ind_pd_df.columns = [
        "ID",
        "Name",
        "DOB",
        "TierCode",
        "EnrollmentDate",
        "Individual or Corporate",
        "Flight Date",
    ]

    ind_pd_df.to_csv(
        COUNTRY_EXPORT_PATH / "india" / "member_profile_india.csv", index=False
    )


def australia_export_function(spark: SparkSession, australia_df):
    """
    Export the Australia member profile data to an Excel file:

    Dimensions of file:
        - Unique ID - mem_id
        - Member Name - name
        - Tier Type - tier
        - Date of Birth - dob - (format: yyyy-MM-dd)
        - Date of Enrollment - enroll_dt - (format: yyyy-MM-dd)
        - Date of Flight - flight_dt - (format: yyyy-MM-dd)
    """

    australia_pd_df = australia_df.select(
        F.col("mem_id").alias("Unique ID"),
        F.col("name").alias("Member Name"),
        F.col("tier").alias("Tier Type"),
        F.date_format(F.col("dob"), "yyyy-MM-dd").alias("Date of Birth"),
        F.date_format(F.col("enroll_dt"), "yyyy-MM-dd").alias(
            "Date of Enrollment"
        ),
        F.date_format(F.col("flight_dt"), "yyyy-MM-dd").alias("Date of Flight"),
    ).toPandas()

    output_path = COUNTRY_EXPORT_PATH / "australia" / "member_profile_australia.xlsx"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    australia_pd_df.to_excel(output_path, index=False, engine="openpyxl")

COUNTRY_TARGETS = {
    "USA": {
        "country_name": "United States",
        "table_name": "member_profile_usa",
        "path": str(COUNTRY_EXPORT_PATH / "usa"),
        "export_function": usa_export_function
    },
    "IND": {
        "country_name": "India",
        "table_name": "member_profile_india",
        "path": str(COUNTRY_EXPORT_PATH / "india"),
        "export_function": india_export_function
    },
    "AUS": {
        "country_name": "Australia",
        "table_name": "member_profile_australia",
        "path": str(COUNTRY_EXPORT_PATH / "australia"),
        "export_function": australia_export_function
    },
    "PHL": {
        "country_name": "Philippines",
        "table_name": "member_profile_philippines",
        "path": str(COUNTRY_EXPORT_PATH / "philippines"),
        "export_function": None
    },
}


def write_country_targets(spark: SparkSession) -> None:
    target_dataframe = spark.read.format("delta").load(TARGET_MEMBER_PATH)

    for country_code, details in COUNTRY_TARGETS.items():
        country_target_dataframe = target_dataframe.filter(
            F.col("country_code") == country_code
        ).select(COUNTRY_COLUMNS)

        # Why overwrite: Because Members can change their country, and we want to ensure that the exported data is always up-to-date with the latest country information. Overwriting ensures that any changes in the member profiles are reflected in the country-specific exports without retaining outdated records.
        (
            country_target_dataframe.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(details["path"])
        )
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {details["table_name"]}
            USING DELTA
            LOCATION 'file://{details["path"]}'
            """
        )


def export_country_targets(spark: SparkSession) -> None:
    for country_code, details in COUNTRY_TARGETS.items():
        export_function = details.get("export_function")
        if export_function is None:
            print(f"No export function defined for country code: {country_code}. Skipping export.")
            continue

        country_df = spark.read.format("delta").load(details["path"])
        export_function(spark, country_df)

def main() -> None:
    spark = SparkSession.builder.appName("MemberProfileCountryExports").getOrCreate()
    write_country_targets(spark)
    export_country_targets(spark)
    spark.stop()


if __name__ == "__main__":
    main()
