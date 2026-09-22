import shutil
import tempfile
import unittest
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from data_validations import (
    add_member_structure_validation_errors,
    add_member_validation_errors,
    add_redemption_validation_errors,
    assert_unique_keys,
    latest_redemption_records,
    write_quarantine,
)


MEMBER_SCHEMA = T.StructType(
    [
        T.StructField("source_file_date", T.DateType(), True),
        T.StructField("field_count", T.IntegerType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("mem_id", T.StringType(), True),
        T.StructField("enroll_dt_raw", T.StringType(), True),
        T.StructField("enroll_dt", T.DateType(), True),
        T.StructField("flight_dt_raw", T.StringType(), True),
        T.StructField("flight_dt", T.DateType(), True),
        T.StructField("tier", T.StringType(), True),
        T.StructField("agent_name", T.StringType(), True),
        T.StructField("state", T.StringType(), True),
        T.StructField("country_code", T.StringType(), True),
        T.StructField("dob_raw", T.StringType(), True),
        T.StructField("dob", T.DateType(), True),
        T.StructField("flag", T.StringType(), True),
    ]
)

STRUCTURE_SCHEMA = T.StructType(
    [
        T.StructField("source_file_date", T.DateType(), True),
        T.StructField("raw_record", T.StringType(), True),
        T.StructField("record_type", T.StringType(), True),
        T.StructField("field_count", T.IntegerType(), True),
    ]
)

REDEMPTION_SCHEMA = T.StructType(
    [
        T.StructField("member_id", T.StringType(), True),
        T.StructField("txn_id", T.StringType(), True),
        T.StructField("feed_date_raw", T.StringType(), True),
        T.StructField("feed_date", T.DateType(), True),
        T.StructField("txn_date_raw", T.StringType(), True),
        T.StructField("txn_date", T.DateType(), True),
        T.StructField("partner", T.StringType(), True),
        T.StructField("miles_redeemed", T.LongType(), True),
        T.StructField("status", T.StringType(), True),
    ]
)

RANKING_SCHEMA = T.StructType(
    [
        T.StructField("member_id", T.StringType(), True),
        T.StructField("txn_id", T.StringType(), True),
        T.StructField("feed_date", T.DateType(), True),
        T.StructField("ingestion_time", T.TimestampType(), True),
        T.StructField("source_path", T.StringType(), True),
        T.StructField("status", T.StringType(), True),
    ]
)


class DataValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("DataValidationTests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def valid_member_row(self):
        return (
            date(2024, 1, 15),
            12,
            "Elena Torres",
            "100000000001",
            "20180514",
            date(2018, 5, 14),
            "20240110",
            date(2024, 1, 10),
            "GLD",
            "Maya",
            "CA",
            "USA",
            "19850503",
            date(1985, 5, 3),
            "A",
        )

    def member_dataframe(self, row=None):
        return self.spark.createDataFrame(
            [row or self.valid_member_row()], MEMBER_SCHEMA
        )

    def test_mandatory_member_fields(self):
        row = list(self.valid_member_row())
        row[2] = None
        row[3] = None
        row[4] = None
        row[5] = None

        result = add_member_validation_errors(self.member_dataframe(tuple(row))).first()

        self.assertIn("MISSING_MEMBER_NAME", result.validation_errors)
        self.assertIn("MISSING_MEMBER_ID", result.validation_errors)
        self.assertIn("MISSING_ENROLLMENT_DATE", result.validation_errors)

    def test_invalid_dates(self):
        row = list(self.valid_member_row())
        row[4] = "20241301"
        row[5] = None
        row[6] = "20241340"
        row[7] = None
        row[12] = "19851303"
        row[13] = None

        result = add_member_validation_errors(self.member_dataframe(tuple(row))).first()

        self.assertIn("INVALID_ENROLLMENT_DATE", result.validation_errors)
        self.assertIn("INVALID_LAST_FLIGHT_DATE", result.validation_errors)
        self.assertIn("INVALID_DATE_OF_BIRTH", result.validation_errors)

    def test_invalid_member_field_count(self):
        dataframe = self.spark.createDataFrame(
            [(date(2024, 1, 15), "|D|Elena", "D", 13)], STRUCTURE_SCHEMA
        )

        result = add_member_structure_validation_errors(
            dataframe,
            "|H|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|Tier_Code|"
            "Agent_Name|State|Country|DOB|Is_Active",
        ).first()

        self.assertIn("INVALID_MEMBER_FIELD_COUNT", result.validation_errors)

    def test_unsupported_country(self):
        row = list(self.valid_member_row())
        row[11] = "CAN"

        result = add_member_validation_errors(self.member_dataframe(tuple(row))).first()

        self.assertIn("UNSUPPORTED_COUNTRY_CODE", result.validation_errors)

    def test_duplicate_member_keys(self):
        dataframe = self.spark.createDataFrame(
            [("100000000001",), ("100000000001",)], ["mem_id"]
        )

        with self.assertRaisesRegex(ValueError, "member_profile_target"):
            assert_unique_keys(dataframe, ["mem_id"], "member_profile_target")

    def test_duplicate_redemption_keys(self):
        dataframe = self.spark.createDataFrame(
            [("100000000001", "RX1"), ("100000000001", "RX1")],
            ["member_id", "txn_id"],
        )

        with self.assertRaisesRegex(ValueError, "redemption_transactions_target"):
            assert_unique_keys(
                dataframe,
                ["member_id", "txn_id"],
                "redemption_transactions_target",
            )

    def test_completed_has_priority_over_pending(self):
        dataframe = self.spark.createDataFrame(
            [
                (
                    "100000000001",
                    "RX1",
                    date(2024, 1, 20),
                    datetime(2024, 1, 20, 1, 0),
                    "/feed/pending.json",
                    "PENDING",
                ),
                (
                    "100000000001",
                    "RX1",
                    date(2024, 1, 15),
                    datetime(2024, 1, 15, 1, 0),
                    "/feed/completed.json",
                    "COMPLETED",
                ),
            ],
            RANKING_SCHEMA,
        )

        result = latest_redemption_records(dataframe).collect()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "COMPLETED")

    def test_quarantine_write(self):
        row = list(self.valid_member_row())
        row[11] = "CAN"
        invalid_dataframe = add_member_validation_errors(
            self.member_dataframe(tuple(row))
        )
        quarantine_path = tempfile.mkdtemp(prefix="member-quarantine-test-")

        try:
            write_quarantine(invalid_dataframe, quarantine_path)
            result = self.spark.read.format("delta").load(quarantine_path)

            self.assertEqual(result.count(), 1)
            self.assertIn(
                "UNSUPPORTED_COUNTRY_CODE", result.first().validation_errors
            )
            self.assertIsNotNone(result.first().validation_time)
        finally:
            shutil.rmtree(quarantine_path, ignore_errors=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
