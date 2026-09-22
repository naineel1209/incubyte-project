# SkyPoints Data Engineering Assessment

This project implements the SkyPoints data flow with Apache Spark and Delta Lake.

The local runtime uses Docker and a shared host directory.

The runtime does not use Snowflake or MinIO.

## Architecture

The flow has these layers:

1. Member profile raw Delta data.
2. Member profile staging Delta data.
3. Latest member profile target Delta data.
4. Country-specific Delta targets and exports.
5. Flattened redemption transaction Delta data.
6. A member and redemption query view.

The member target stores one latest row per `mem_id`.

The redemption target stores one logical row per `(member_id, txn_id)`.

`COMPLETED` has higher status priority than `PENDING`.

## Technical Assessment Notes

The code contains meaningful comments that explain important processing decisions.

Comments marked `Technical Assessment:` identify the related assessment deliverables.

Search the repository for `Technical Assessment:` to find these comments.

## Prerequisites

Install Docker and Docker Compose.

The commands below run from the repository root.

The Spark image installs `pandas` and `openpyxl` for country exports.

## Complete Flow

### 1. Start Spark

```bash
make spark-up
make spark-ps
```

Open the Spark master interface at `http://localhost:18080`.

### 2. Create Delta Tables

Create the member profile tables:

```bash
docker compose exec -T spark-master \
  spark-sql \
  --master 'local[2]' \
  -f /opt/skypoints/jobs/spark/sql/member_profile_ddl.sql
```

Create the redemption tables:

```bash
docker compose exec -T spark-master \
  spark-sql \
  --master 'local[2]' \
  -f /opt/skypoints/jobs/spark/sql/redemption_ddl.sql
```

The ingestion jobs also create their Delta paths when needed.

### 3. Ingest Member Profiles

Member files must exist in:

```text
data/incoming/member/
```

Run the member ingestion job:

```bash
make spark-submit-member-profile
```

The job reads each unprocessed file once.

The control table stores processed member file paths.

The target merge keeps the newest member record.

### 4. Ingest Redemption JSON

Redemption files must exist in:

```text
data/incoming/redemption/
```

Run the redemption ingestion job:

```bash
make spark-submit-redemption-ingestion
```

The job explodes each `redemptions` array into transaction rows.

The control table stores processed redemption file paths.

The merge key is `(member_id, txn_id)`.

### 5. Create Country Targets

The member ingestion job creates the country Delta targets automatically.

Run the export job again when you need to rebuild country outputs:

```bash
make spark-submit-member-exports
```

The country Delta locations are:

```text
data/exports/member/usa/
data/exports/member/india/
data/exports/member/australia/
data/exports/member/philippines/
```

The Australia Excel export is:

```text
data/exports/member/australia/member_profile_australia.xlsx
```

### 6. Query The Data

The global member target is stored at:

```text
data/delta/target/member_profile/
```

The flattened redemption target is stored at:

```text
data/delta/target/redemption_transactions/
```

Query member profiles:

```bash
docker compose exec -T spark-master \
  spark-sql \
  --master 'local[2]' \
  -e 'SELECT * FROM delta.`file:///opt/skypoints-data/delta/target/member_profile` LIMIT 20;'
```

Query flattened redemption transactions:

```bash
docker compose exec -T spark-master \
  spark-sql \
  --master 'local[2]' \
  -e 'SELECT * FROM delta.`file:///opt/skypoints-data/delta/target/redemption_transactions` LIMIT 20;'
```

Join transactions to the latest member profile:

```bash
docker compose exec -T spark-master \
  spark-sql \
  --master 'local[2]' \
  -e 'SELECT
        redemption.member_id,
        redemption.txn_id,
        redemption.feed_date,
        redemption.txn_date,
        redemption.partner,
        redemption.miles_redeemed,
        redemption.status,
        member.name,
        member.country_code,
        member.tier,
        member.stale_member
      FROM delta.`file:///opt/skypoints-data/delta/target/redemption_transactions` redemption
      LEFT JOIN delta.`file:///opt/skypoints-data/delta/target/member_profile` member
        ON redemption.member_id = member.mem_id;'
```

The join uses `redemption.member_id = member.mem_id`.

The `LEFT JOIN` keeps transactions with missing member profiles.

Create the session view with:

```bash
docker compose exec -T spark-master \
  spark-sql \
  --master local[2] \
  -f /opt/skypoints/jobs/spark/sql/redemption_queries.sql
```

The view definition is in `jobs/spark/sql/redemption_queries.sql`.

The direct Delta path query remains the most reliable local query method.

## Smoke Test

The smoke test writes this path:

```text
data/delta/smoke/member_profile/
```

Run it with:

```bash
make spark-submit-smoke
```

## Stop Spark

```bash
make spark-down
```

## Decision Handbook

Read `docs/decision-handbook.txt` before changing the data flow.

The handbook records accepted source, layer, storage, country, and export decisions.
