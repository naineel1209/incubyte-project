-- Technical Assessment: Deliverable 5: Data validation results.

SELECT
    validation_errors,
    COUNT(*) AS invalid_row_count
FROM delta.`file:///opt/skypoints-data/quarantine/member_profile`
GROUP BY validation_errors
ORDER BY invalid_row_count DESC;

SELECT
    validation_errors,
    COUNT(*) AS invalid_row_count
FROM delta.`file:///opt/skypoints-data/quarantine/redemption_transactions`
GROUP BY validation_errors
ORDER BY invalid_row_count DESC;

SELECT
    mem_id,
    COUNT(*) AS duplicate_count
FROM delta.`file:///opt/skypoints-data/delta/target/member_profile`
GROUP BY mem_id
HAVING COUNT(*) > 1;

SELECT
    member_id,
    txn_id,
    COUNT(*) AS duplicate_count
FROM delta.`file:///opt/skypoints-data/delta/target/redemption_transactions`
GROUP BY member_id, txn_id
HAVING COUNT(*) > 1;
