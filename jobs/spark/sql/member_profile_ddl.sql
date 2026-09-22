-- Technical Assessment: Deliverable 1: Separate DDL of each tables we are touching, second option is to use a stable script controlled table creation with schema management in script itself
CREATE TABLE IF NOT EXISTS member_profile_raw (
    source_path STRING,
    raw_record STRING,
    source_file_date DATE,
    ingestion_time TIMESTAMP
)
USING DELTA
LOCATION 'file:///opt/skypoints-data/delta/raw/member_profile';

CREATE TABLE IF NOT EXISTS member_profile_staging (
    source_path STRING,
    ingestion_time TIMESTAMP,
    source_file_date DATE,
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
    ingestion_time TIMESTAMP,
    source_file_date DATE
)
USING DELTA
PARTITIONED BY (country_code)
LOCATION 'file:///opt/skypoints-data/delta/target/member_profile';
