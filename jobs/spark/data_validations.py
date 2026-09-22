from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


MEMBER_QUARANTINE_PATH = "/opt/skypoints-data/quarantine/member_profile"
REDEMPTION_QUARANTINE_PATH = (
    "/opt/skypoints-data/quarantine/redemption_transactions"
)
SUPPORTED_COUNTRY_CODES = ("USA", "IND", "AUS", "PHL")


def validation_error(condition, code):
    empty_errors = F.expr("array()").cast("array<string>")
    return F.when(condition, F.array(F.lit(code))).otherwise(empty_errors)


def combine_validation_errors(*errors):
    return F.array_distinct(F.flatten(F.array(*errors)))


def add_member_structure_validation_errors(
    member_dataframe: DataFrame, expected_header: str
) -> DataFrame:
    errors = combine_validation_errors(
        validation_error(
            F.col("source_file_date").isNull(), "INVALID_SOURCE_FILE_DATE"
        ),
        validation_error(
            F.col("record_type").isNull()
            | (~F.col("record_type").isin("H", "D")),
            "INVALID_RECORD_TYPE",
        ),
        validation_error(
            (F.col("record_type") == "H")
            & (F.trim(F.col("raw_record")) != expected_header),
            "INVALID_MEMBER_HEADER",
        ),
        validation_error(
            (F.col("record_type") == "D") & (F.col("field_count") != 12),
            "INVALID_MEMBER_FIELD_COUNT",
        ),
    )
    return member_dataframe.withColumn("validation_errors", errors)


def add_member_validation_errors(member_dataframe: DataFrame) -> DataFrame:
    current_date = F.current_date()
    name = F.trim(F.col("name"))
    mem_id = F.trim(F.col("mem_id"))
    tier = F.trim(F.col("tier"))
    agent_name = F.trim(F.col("agent_name"))
    state = F.trim(F.col("state"))
    country_code = F.upper(F.trim(F.col("country_code")))
    flag = F.upper(F.trim(F.col("flag")))

    errors = combine_validation_errors(
        validation_error(
            F.col("source_file_date").isNull(), "INVALID_SOURCE_FILE_DATE"
        ),
        validation_error(
            F.col("field_count") != 12, "INVALID_MEMBER_FIELD_COUNT"
        ),
        validation_error(
            name.isNull() | (F.length(name) == 0), "MISSING_MEMBER_NAME"
        ),
        validation_error(
            F.length(name) > 255, "MEMBER_NAME_TOO_LONG"
        ),
        validation_error(
            mem_id.isNull() | (F.length(mem_id) == 0), "MISSING_MEMBER_ID"
        ),
        validation_error(
            (F.length(mem_id) > 18) | (~mem_id.rlike(r"^[0-9]+$")),
            "INVALID_MEMBER_ID",
        ),
        validation_error(
            F.col("enroll_dt_raw").isNotNull()
            & (F.length(F.trim(F.col("enroll_dt_raw"))) > 0)
            & F.col("enroll_dt").isNull(),
            "INVALID_ENROLLMENT_DATE",
        ),
        validation_error(
            F.col("enroll_dt").isNull(), "MISSING_ENROLLMENT_DATE"
        ),
        validation_error(
            F.col("enroll_dt").isNotNull() & (F.col("enroll_dt") > current_date),
            "FUTURE_ENROLLMENT_DATE",
        ),
        validation_error(
            F.col("flight_dt_raw").isNotNull()
            & (F.length(F.trim(F.col("flight_dt_raw"))) > 0)
            & F.col("flight_dt").isNull(),
            "INVALID_LAST_FLIGHT_DATE",
        ),
        validation_error(
            F.col("flight_dt").isNotNull() & (F.col("flight_dt") > current_date),
            "FUTURE_LAST_FLIGHT_DATE",
        ),
        validation_error(
            F.col("flight_dt").isNotNull()
            & F.col("enroll_dt").isNotNull()
            & (F.col("flight_dt") < F.col("enroll_dt")),
            "FLIGHT_BEFORE_ENROLLMENT",
        ),
        validation_error(
            F.col("dob_raw").isNotNull()
            & (F.length(F.trim(F.col("dob_raw"))) > 0)
            & F.col("dob").isNull(),
            "INVALID_DATE_OF_BIRTH",
        ),
        validation_error(
            F.col("dob").isNotNull() & (F.col("dob") > current_date),
            "FUTURE_DATE_OF_BIRTH",
        ),
        validation_error(
            F.col("dob").isNotNull()
            & (
                (F.floor(F.months_between(current_date, F.col("dob")) / 12) < 0)
                | (F.floor(F.months_between(current_date, F.col("dob")) / 12) > 120)
            ),
            "IMPLAUSIBLE_MEMBER_AGE",
        ),
        validation_error(
            F.length(tier) > 5, "TIER_CODE_TOO_LONG"
        ),
        validation_error(
            F.length(agent_name) > 255, "AGENT_NAME_TOO_LONG"
        ),
        validation_error(
            F.length(state) > 5, "STATE_TOO_LONG"
        ),
        validation_error(
            country_code.isNull() | (F.length(country_code) == 0),
            "MISSING_COUNTRY_CODE",
        ),
        validation_error(
            country_code.isNotNull()
            & (~country_code.isin(*SUPPORTED_COUNTRY_CODES)),
            "UNSUPPORTED_COUNTRY_CODE",
        ),
        validation_error(
            flag.isNotNull() & (F.length(flag) > 0) & (~flag.isin("A", "I")),
            "INVALID_ACTIVE_MEMBER_FLAG",
        ),
    )
    return member_dataframe.withColumn("validation_errors", errors)


def add_redemption_validation_errors(
    redemption_dataframe: DataFrame,
) -> DataFrame:
    current_date = F.current_date()
    member_id = F.trim(F.col("member_id"))
    txn_id = F.trim(F.col("txn_id"))
    partner = F.trim(F.col("partner"))
    status = F.upper(F.trim(F.col("status")))

    errors = combine_validation_errors(
        validation_error(
            F.col("feed_date_raw").isNotNull()
            & (F.length(F.trim(F.col("feed_date_raw"))) > 0)
            & F.col("feed_date").isNull(),
            "INVALID_FEED_DATE",
        ),
        validation_error(
            F.col("feed_date").isNull(), "MISSING_FEED_DATE"
        ),
        validation_error(
            F.col("feed_date").isNotNull() & (F.col("feed_date") > current_date),
            "FUTURE_FEED_DATE",
        ),
        validation_error(
            member_id.isNull() | (F.length(member_id) == 0), "MISSING_MEMBER_ID"
        ),
        validation_error(
            (F.length(member_id) > 18) | (~member_id.rlike(r"^[0-9]+$")),
            "INVALID_MEMBER_ID",
        ),
        validation_error(
            txn_id.isNull() | (F.length(txn_id) == 0), "MISSING_TRANSACTION_ID"
        ),
        validation_error(
            F.col("txn_date_raw").isNotNull()
            & (F.length(F.trim(F.col("txn_date_raw"))) > 0)
            & F.col("txn_date").isNull(),
            "INVALID_TRANSACTION_DATE",
        ),
        validation_error(
            F.col("txn_date").isNull(), "MISSING_TRANSACTION_DATE"
        ),
        validation_error(
            F.col("txn_date").isNotNull()
            & F.col("feed_date").isNotNull()
            & (F.col("txn_date") > F.col("feed_date")),
            "TRANSACTION_AFTER_FEED_DATE",
        ),
        validation_error(
            partner.isNull() | (F.length(partner) == 0), "MISSING_PARTNER"
        ),
        validation_error(
            F.length(partner) > 255, "PARTNER_TOO_LONG"
        ),
        validation_error(
            F.col("miles_redeemed").isNull(), "MISSING_MILES_REDEEMED"
        ),
        validation_error(
            F.col("miles_redeemed").isNotNull()
            & (F.col("miles_redeemed") < 0),
            "NEGATIVE_MILES_REDEEMED",
        ),
        validation_error(
            status.isNull() | (F.length(status) == 0), "MISSING_REDEMPTION_STATUS"
        ),
        validation_error(
            status.isNotNull()
            & (F.length(status) > 0)
            & (~status.isin("PENDING", "COMPLETED")),
            "INVALID_REDEMPTION_STATUS",
        ),
    )
    return redemption_dataframe.withColumn("validation_errors", errors)


def latest_member_records(member_dataframe: DataFrame) -> DataFrame:
    ranking_window = Window.partitionBy("mem_id").orderBy(
        F.col("source_file_date").desc(),
        F.col("ingestion_time").desc(),
        F.col("source_path").desc(),
    )
    return (
        member_dataframe.withColumn(
            "row_number", F.row_number().over(ranking_window)
        )
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )


def latest_redemption_records(redemption_dataframe: DataFrame) -> DataFrame:
    status_priority = (
        F.when(F.col("status") == "COMPLETED", F.lit(2))
        .when(F.col("status") == "PENDING", F.lit(1))
        .otherwise(F.lit(0))
    )
    ranking_window = Window.partitionBy("member_id", "txn_id").orderBy(
        status_priority.desc(),
        F.col("feed_date").desc_nulls_last(),
        F.col("ingestion_time").desc_nulls_last(),
        F.col("source_path").desc(),
    )
    return (
        redemption_dataframe.withColumn(
            "row_number", F.row_number().over(ranking_window)
        )
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )


def write_quarantine(invalid_dataframe: DataFrame, quarantine_path: str) -> None:
    if invalid_dataframe.limit(1).count() == 0:
        return

    (
        invalid_dataframe.withColumn("validation_time", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(quarantine_path)
    )


def assert_unique_keys(
    dataframe: DataFrame, key_columns, dataset_name: str
) -> None:
    duplicate_keys = (
        dataframe.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
    )
    if duplicate_keys.count() > 0:
        key_description = ", ".join(key_columns)
        raise ValueError(
            f"{dataset_name} contains duplicate keys for columns: {key_description}"
        )
