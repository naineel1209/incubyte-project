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

-- Technical Assessment: Deliverable 5: Invalid redemption rows remain queryable in quarantine.
CREATE TABLE IF NOT EXISTS redemption_transactions_quarantine (
    source_path STRING,
    member_id STRING,
    feed_date_raw STRING,
    feed_date DATE,
    txn_id STRING,
    txn_date_raw STRING,
    txn_date DATE,
    partner STRING,
    miles_redeemed BIGINT,
    status STRING,
    _corrupt_record STRING,
    validation_errors ARRAY<STRING>,
    validation_time TIMESTAMP
)
USING DELTA
LOCATION 'file:///opt/skypoints-data/quarantine/redemption_transactions';
