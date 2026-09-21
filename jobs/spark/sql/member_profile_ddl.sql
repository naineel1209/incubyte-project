CREATE TABLE IF NOT EXISTS member_profile_raw (
    source_path STRING,
    raw_record STRING,
    ingestion_time TIMESTAMP
)
USING DELTA
LOCATION 'file:///opt/skypoints-data/delta/raw/member_profile';

CREATE TABLE IF NOT EXISTS member_profile_staging (
    source_path STRING,
    ingestion_time TIMESTAMP,
    name STRING,
    mem_id STRING,
    enroll_dt DATE,
    flight_dt DATE,
    tier STRING,
    agent_name STRING,
    state STRING,
    country_code STRING,
    dob DATE,
    flag STRING,
    age INT,
    stale_member BOOLEAN
)
USING DELTA
LOCATION 'file:///opt/skypoints-data/delta/staging/member_profile';

CREATE TABLE IF NOT EXISTS member_profile_target (
    mem_id STRING,
    name STRING,
    enroll_dt DATE,
    flight_dt DATE,
    tier STRING,
    agent_name STRING,
    state STRING,
    country_code STRING,
    dob DATE,
    flag STRING,
    age INT,
    stale_member BOOLEAN,
    source_path STRING,
    ingestion_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (country_code)
LOCATION 'file:///opt/skypoints-data/delta/target/member_profile';
