CREATE TABLE IF NOT EXISTS redemption_file_control (
    source_path STRING
)
USING DELTA
LOCATION 'file:///opt/skypoints-data/control/redemption_file_control';

CREATE TABLE IF NOT EXISTS redemption_transactions (
    member_id STRING,
    txn_id STRING,
    feed_date DATE,
    txn_date DATE,
    partner STRING,
    miles_redeemed BIGINT,
    status STRING,
    source_path STRING,
    ingestion_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (feed_date)
LOCATION 'file:///opt/skypoints-data/delta/target/redemption_transactions';
