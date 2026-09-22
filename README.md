# SkyPoints Data Engineering Assessment

This project implements the assessment with Spark and Delta Lake.

The local runtime uses Docker and a shared host directory.

The local runtime does not use Snowflake or MinIO.

## Current Foundation

The current foundation contains:

- A Spark master container.
- A Spark worker container.
- A shared local storage mount.
- Delta Lake package configuration.
- A Delta write and read smoke job.
- A decision handbook.

## Start Spark

```bash
make spark-up
make spark-ps
```

Open the Spark master interface at `http://localhost:18080`.

## Run the Delta Smoke Test

```bash
make spark-submit-smoke
```

The job writes this path:

```text
data/delta/smoke/member_profile/
```

The job reads the Delta data and prints the row count.

## Stop Spark

```bash
make spark-down
```

## Input Directories

Place member flat files in:

```text
data/incoming/member/
```

Place redemption JSON files in:

```text
data/incoming/redemption/
```

## Decision Handbook

Read `docs/decision-handbook.txt` before changing the data flow.

The handbook records the accepted source, layer, storage, country, and export decisions.

## Country Targets

The ingestion job writes the global target to:

```text
data/delta/target/member_profile/
```

It also writes country-specific Delta targets to:

```text
data/exports/member/usa/
data/exports/member/india/
data/exports/member/australia/
data/exports/member/philippines/
```

Run the standalone export job with:

```bash
make spark-submit-member-exports
```

Query a country target from Spark SQL with:

```sql
SELECT *
FROM delta.`file:///opt/skypoints-data/exports/member/usa`;
```
