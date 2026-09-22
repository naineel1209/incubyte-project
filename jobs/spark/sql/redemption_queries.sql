-- Technical Assessment: Deliverable 4: Joining Redemption Data back to Member Data and fill in details

CREATE OR REPLACE VIEW member_redemption_profile AS
SELECT
    redemption.member_id,
    redemption.txn_id,
    redemption.feed_date,
    redemption.txn_date,
    redemption.partner,
    redemption.miles_redeemed,
    redemption.status,
    member.name,
    member.enroll_dt,
    member.flight_dt,
    member.tier,
    member.agent_name,
    member.state,
    member.country_code,
    member.dob,
    member.flag,
    member.age,
    member.stale_member
FROM delta.`file:///opt/skypoints-data/delta/target/redemption_transactions` redemption
LEFT JOIN delta.`file:///opt/skypoints-data/delta/target/member_profile` member
    ON redemption.member_id = member.mem_id;
